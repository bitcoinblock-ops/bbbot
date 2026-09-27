#!/usr/bin/env python3
"""
Bitcoin Block - Bot de distribuicao de noticias para grupos do Telegram.
@bitcoin_block_bot                                              versao 2026-09-26

Fluxo:
  1. Dono de um grupo adiciona o bot e da permissao de postar.
     -> O bot se auto-registra (handler my_chat_member).
  2. Distribuicao (duas entradas, mesmo caminho):
     a) MANUAL: voce posta um link no topico "BOT DISTRIBUICAO" do grupo fonte.
     b) AUTOMATICA: de segunda a sexta as 10:05, 14:05 e 18:05, e no sabado e
        domingo so as 12:05 (Brasilia), o bot le o RSS do bitcoinblock.com.br,
        pega a ULTIMA noticia publicada, posta o link no topico fonte e distribui.
     -> O bot COPIA a mensagem (link + previa) para TODOS os grupos.
  3. O dono roda  /vincular SEU_ID  dentro do grupo.
     -> O bot liga o grupo a conta BBDAO do dono (airdrop de parceiros).

Regra do envio automatico (por horario):
  - "Noticia do horario" = a mais nova do RSS publicada ate 60 min antes do horario.
  - Se ela ja foi distribuida (voce postou na mao), o horario e pulado.
  - Se ainda nao apareceu no RSS, o bot espera ate AUTO_WAIT_MIN minutos.
  - Depois disso, manda a noticia mais nova ainda nao enviada (ate AUTO_MAX_AGE_H horas);
    se nao houver nenhuma, pula o horario. Nunca repete um link ja enviado.
  - Categorias em AUTO_SKIP_CATEGORIES (padrao: Imprensa) sao ignoradas.

Variaveis de ambiente (Coolify, somente Runtime):
  obrigatorias: TELEGRAM_TOKEN, SOURCE_CHAT_ID, SOURCE_THREAD_ID, ADMIN_CHAT_ID
  opcionais   : BBDAO_API_URL, BBDAO_API_KEY, SEED_GROUPS_JSON, ALLOWED_DOMAINS,
                SEND_DELAY (1.0), FAIL_ALERT_AFTER (3),
                AUTO_ENABLED (1), AUTO_TIMES ("10:05,14:05,18:05", seg-sex, horario de Brasilia),
                AUTO_TIMES_WEEKEND ("12:05", sab-dom; vazio = nada automatico no fim de semana),
                RSS_URL (https://bitcoinblock.com.br/feed/), AUTO_SKIP_CATEGORIES ("Imprensa"),
                AUTO_WAIT_MIN (10), AUTO_MAX_AGE_H (24)

Robustez:
  - Estado persistido em disco (offset + mensagens/links ja enviados) => sem spam ao reiniciar.
  - Backlog ignorado no 1o boot => nao re-dispara links velhos.
  - Retry/backoff em erro de rede + tratamento de 429 (rate limit).
  - Token e API key MASCARADOS em qualquer linha de log.
  - 401/404 no getUpdates (token revogado/errado) derruba o processo com mensagem clara.
  - Contador de falhas por grupo + alerta no chat admin na 3a falha seguida.
  - Remove automaticamente grupos onde o bot foi expulso/bloqueado (403).
  - Atualiza titulos/membros 1x/dia; relatorio mensal com nova tentativa se falhar.
"""

import os
import re
import json
import time
import logging
import logging.handlers
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse

import requests

# ----------------------------------------------------------------- Config
try:
    from infos import infos
except Exception:
    infos = {}

TOKEN          = os.environ.get("TELEGRAM_TOKEN")      or infos.get("telegram_token")
SOURCE_CHAT_ID = int(os.environ.get("SOURCE_CHAT_ID")    or infos.get("chat_id"))
SOURCE_THREAD  = int(os.environ.get("SOURCE_THREAD_ID")  or infos.get("message_thread_id"))
ADMIN_CHAT_ID  = os.environ.get("ADMIN_CHAT_ID")      or infos.get("admin_group_id")

if not TOKEN:
    raise SystemExit("Defina TELEGRAM_TOKEN (env) ou telegram_token em infos.py")

API         = f"https://api.telegram.org/bot{TOKEN}"
GROUPS_FILE = os.environ.get("GROUPS_FILE", "group_ids.json")
STATE_FILE  = os.environ.get("STATE_FILE",  "bot_state.json")
LOG_FILE    = os.environ.get("LOG_FILE",    "logBot.txt")

# Vinculo grupo <-> conta BBDAO (comando /vincular). Sem isso, o /vincular ainda
# grava o vinculo localmente (group_ids.json) e avisa o admin, mas NAO confirma na plataforma.
BBDAO_API_URL = (os.environ.get("BBDAO_API_URL") or "").rstrip("/")   # ex: https://bbdao.digital/api/v1
BBDAO_API_KEY = os.environ.get("BBDAO_API_KEY") or ""                 # = bbdao_api_key do /private/secrets.php

SEND_DELAY        = float(os.environ.get("SEND_DELAY", "1.0"))  # s entre envios (folga no limite ~30/s)
DETAILS_REFRESH_S = 24 * 3600     # atualizar metadados dos grupos 1x/dia
MAX_SEEN          = 500           # quantos message_ids lembrar p/ dedup
MAX_SENT_URLS     = 300           # quantos links lembrar p/ nunca repetir noticia
FAIL_ALERT_AFTER  = int(os.environ.get("FAIL_ALERT_AFTER", "3"))

# ----------------------------------------------------------------- Log (com segredos mascarados)
_TOKEN_RE = re.compile(r"\d{6,12}:[A-Za-z0-9_-]{30,}")

def _safe(x):
    """Remove token/API key de qualquer texto antes de ir para o log."""
    s = str(x)
    for sec in (TOKEN, BBDAO_API_KEY):
        if sec and len(sec) >= 8:
            s = s.replace(sec, "<REDACTED>")
    return _TOKEN_RE.sub("<TOKEN-REDACTED>", s)

class RedactingFormatter(logging.Formatter):
    def format(self, record):
        return _safe(super().format(record))

_fmt = RedactingFormatter("%(asctime)s %(levelname)s %(message)s")
_handlers = [logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=3),
             logging.StreamHandler()]
for _h in _handlers:
    _h.setFormatter(_fmt)
logging.basicConfig(level=logging.INFO, handlers=_handlers)
logging.getLogger("urllib3").setLevel(logging.WARNING)
log = logging.getLogger("bbbot")

def _now():
    return datetime.now(timezone.utc)

# ----------------------------------------------------------------- Persistencia
def _load(path, default):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default

def _save(path, data):
    tmp = f"{path}.tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, path)   # gravacao atomica: nunca corrompe o arquivo

def load_groups():       return _load(GROUPS_FILE, [])
def save_groups(g):      _save(GROUPS_FILE, g)

def seed_groups_if_needed():
    """Inicializa a lista quando ela esta vazia. O grupo FONTE nunca conta como
    destino (auto-cura listas onde ele entrou por engano). So semeia se, fora a
    fonte, a lista estiver vazia."""
    existing = [g for g in load_groups() if g.get("group_id") != SOURCE_CHAT_ID]
    if existing:
        save_groups(existing)    # remove a fonte se tiver entrado por engano
        return
    raw = os.environ.get("SEED_GROUPS_JSON")
    if raw:
        try:
            groups = json.loads(raw)
            save_groups(groups)
            log.info("group_ids inicializado via SEED_GROUPS_JSON (%d grupos)", len(groups))
            return
        except Exception:
            log.exception("SEED_GROUPS_JSON invalido; ignorando")
    seed = os.environ.get("SEED_GROUPS_FILE", "group_ids.seed.json")
    if os.path.exists(seed):
        save_groups(_load(seed, []))
        log.info("group_ids inicializado a partir de %s", seed)
def load_state():        return _load(STATE_FILE, {"offset": None, "seen": [], "last_details": 0, "last_report": ""})
def save_state(s):       _save(STATE_FILE, s)

# ----------------------------------------------------------------- Telegram API
def api_raw(method, params=None, http="get", timeout=30):
    """Chama a API com retry de rede e tratamento de 429. Devolve o JSON cru."""
    url = f"{API}/{method}"
    for attempt in range(5):
        try:
            if http == "get":
                r = requests.get(url, params=params, timeout=timeout)
            else:
                r = requests.post(url, data=params, timeout=timeout)
            data = r.json()
            if data.get("ok"):
                return data
            if data.get("error_code") == 429:
                wait = data.get("parameters", {}).get("retry_after", 5) + 1
                log.warning("429 em %s: aguardando %ss", method, wait)
                time.sleep(wait)
                continue
            return data  # erro definitivo (403/400/...) -> quem chamou decide
        except requests.exceptions.RequestException as e:
            wait = min(60, 3 * 2 ** attempt)
            # so o TIPO do erro: a mensagem da excecao traz a URL com o token
            log.warning("Rede falhou em %s (%s); retry em %ss", method, type(e).__name__, wait)
            time.sleep(wait)
    return {"ok": False, "error_code": -1, "description": "falha de rede"}

def api(method, params=None, http="get", timeout=30):
    return api_raw(method, params, http, timeout).get("result")

def notify_admin(text):
    """Avisa o chat admin. Devolve True se entregou; se falhar, registra ERROR no log."""
    if not ADMIN_CHAT_ID:
        log.error("ADMIN_CHAT_ID vazio; aviso perdido: %s", text[:300])
        return False
    ok = True
    for i in range(0, len(text), 3900):          # limite de 4096 caracteres por mensagem
        res = api_raw("sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text[i:i + 3900]}, http="post")
        if not res.get("ok"):
            log.error("notify_admin falhou (%s %s): %s",
                      res.get("error_code"), res.get("description"), text[:200])
            ok = False
    return ok

# ----------------------------------------------------------------- Registro de grupos
def register_group(chat):
    gid = chat["id"]
    if gid == SOURCE_CHAT_ID:
        return                    # o grupo fonte nunca vira destino
    groups = load_groups()
    if any(g["group_id"] == gid for g in groups):
        return
    groups.append({
        "group_id": gid,
        "title": chat.get("title", "Desconhecido"),
        "has_topics": bool(chat.get("is_forum")),
        "selected_thread_id": None,
        "members_count": 0,
        "owner_username": "Desconhecido",
    })
    save_groups(groups)
    log.info("Grupo registrado: %s (%s)", chat.get("title"), gid)
    notify_admin(f"OK - novo grupo na rede: {chat.get('title')} ({gid})")
    # boas-vindas (ignora falha se o bot ainda nao tiver permissao de postar)
    api("sendMessage", {
        "chat_id": gid,
        "text": ("Olá! \U0001F44B A partir de agora este grupo recebe notícias de "
                 "blockchain exclusivas e em primeira mão, direto do BitcoinBlock.com.br "
                 "— conteúdo selecionado, sem spam."),
    }, http="post")

def unregister_group(gid, reason=""):
    groups = load_groups()
    new = [g for g in groups if g["group_id"] != gid]
    if len(new) != len(groups):
        save_groups(new)
        log.info("Grupo removido: %s %s", gid, reason)
        notify_admin(f"X - grupo saiu da rede: {gid} {reason}")

# ----------------------------------------------------------------- Filtro de links
LINK_RE = re.compile(r"https?://\S+")

# So distribui artigos destes dominios (separados por virgula via env).
# Padrao: bitcoinblock.com.br -> evita postar qualquer link por engano nos grupos.
ALLOWED_DOMAINS = [d.strip().lower() for d in
                   os.environ.get("ALLOWED_DOMAINS", "bitcoinblock.com.br").split(",")
                   if d.strip()]

def _host_allowed(link):
    """True so se o HOST do link for um dominio permitido (ou subdominio).
    Bloqueia spoof tipo bitcoinblock.com.br.golpe.io."""
    try:
        host = (urlparse(link).hostname or "").lower().rstrip(".")
    except Exception:
        return False
    return any(host == d or host.endswith("." + d) for d in ALLOWED_DOMAINS)

def allowed_links(msg):
    text = msg.get("text") or msg.get("caption") or ""
    return [link for link in LINK_RE.findall(text) if _host_allowed(link)]

def is_distributable(msg):
    """So distribui se a mensagem tiver um link de um dominio permitido.
    Imagem/print sozinho (sem link permitido) NAO e distribuido."""
    return bool(allowed_links(msg))

def norm_url(u):
    """Chave do link p/ dedup: host sem www + caminho sem barra final (ignora ?utm etc.)."""
    p = urlparse(u.strip())
    host = (p.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host + p.path.rstrip("/")

# ----------------------------------------------------------------- Distribuicao
FALLBACK_ERRORS = ("webpage_forbidden", "not enough rights to send photos")
GONE_ERRORS     = ("kicked", "blocked", "not a member", "chat not found")

def send_text_only(gid, msg, thread=None):
    """Reenvia so o texto/legenda, SEM previa (links preservados). Usado apenas em grupos
    marcados com "fallback_sem_previa": true no group_ids.json (com o ok do dono)."""
    text = msg.get("text") or msg.get("caption") or ""
    if not text:
        return {"ok": False, "error_code": 0, "description": "sem texto p/ fallback"}
    p = {"chat_id": gid, "text": text, "link_preview_options": json.dumps({"is_disabled": True})}
    ents = msg.get("entities") or msg.get("caption_entities")
    if ents:
        p["entities"] = json.dumps(ents)
    if thread:
        p["message_thread_id"] = thread
    return api_raw("sendMessage", p, http="post")

def mark_fail(g, code, desc):
    n = g["fail_streak"] = g.get("fail_streak", 0) + 1
    g["last_error"] = f"{code} {desc}"[:200]
    g.setdefault("fail_since", _now().isoformat())
    log.warning("Falha ao enviar p/ %s (%s): %s %s [%sx seguidas]",
                g["group_id"], g.get("title"), code, desc, n)
    if n == FAIL_ALERT_AFTER or (n > FAIL_ALERT_AFTER and n % 20 == 0):
        notify_admin(f"⚠ {g.get('title')} ({g['group_id']}) falhou {n}x seguidas desde "
                     f"{g['fail_since'][:10]}: {code} {desc}\n"
                     f"Ação: pedir a @{g.get('owner_username', '?')} que promova o bot a ADMIN "
                     f"(sem permissões) ou tirar o grupo da rede.")

def mark_ok(g, via):
    if g.get("fail_streak", 0) >= FAIL_ALERT_AFTER:
        notify_admin(f"✅ {g.get('title')} ({g['group_id']}) voltou a receber (via {via}).")
    g["fail_streak"] = 0
    g.pop("fail_since", None)
    g["last_ok"], g["last_via"] = _now().isoformat(), via

def broadcast(msg):
    """Copia a mensagem do topico fonte para todos os grupos. Devolve (enviados, total)."""
    mid, groups = msg["message_id"], load_groups()
    sent, total, gone = 0, 0, []
    for g in groups:
        gid = g["group_id"]
        if gid == SOURCE_CHAT_ID or g.get("active") is False:
            continue              # nunca ecoa de volta no grupo fonte
        total += 1
        thread = g.get("selected_thread_id") if g.get("has_topics") else None
        p = {"chat_id": gid, "from_chat_id": SOURCE_CHAT_ID, "message_id": mid}
        if thread:
            p["message_thread_id"] = thread
        res, via = api_raw("copyMessage", p, http="post"), "copia"
        desc = (res.get("description") or "").lower()
        # sem previa SO onde o dono autorizou: "fallback_sem_previa": true no group_ids.json
        if not res.get("ok") and g.get("fallback_sem_previa") and any(k in desc for k in FALLBACK_ERRORS):
            res, via = send_text_only(gid, msg, thread), "texto"
            desc = (res.get("description") or "").lower() or desc
        code = res.get("error_code")
        if res.get("ok"):
            sent += 1
            mark_ok(g, via)
        elif code == 403 or any(k in desc for k in GONE_ERRORS):
            gone.append((gid, f"({code}: {desc})"))
        else:
            mark_fail(g, code, desc)
        time.sleep(SEND_DELAY)
    save_groups(groups)          # grava os contadores ANTES de remover
    for gid, why in gone:
        unregister_group(gid, why)
    log.info("Mensagem %s distribuida para %s/%s grupos", mid, sent, total)
    return sent, total

def distribute(msg, state):
    """Marca como enviada ANTES de distribuir (se cair no meio, nao duplica) e distribui."""
    state["seen"] = (state.get("seen", []) + [msg["message_id"]])[-MAX_SEEN:]
    urls = state.get("sent_urls", [])
    for link in allowed_links(msg):
        key = norm_url(link)
        if key not in urls:
            urls.append(key)
    state["sent_urls"] = urls[-MAX_SENT_URLS:]
    save_state(state)
    return broadcast(msg)

# ----------------------------------------------------------------- Envio automatico (RSS)
BRT            = timezone(timedelta(hours=-3))    # Brasilia (sem horario de verao desde 2019)
AUTO_ENABLED   = os.environ.get("AUTO_ENABLED", "1").strip() == "1"
AUTO_TIMES     = [t.strip() for t in os.environ.get("AUTO_TIMES", "10:05,14:05,18:05").split(",") if t.strip()]          # seg-sex
AUTO_TIMES_WEEKEND = [t.strip() for t in os.environ.get("AUTO_TIMES_WEEKEND", "12:05").split(",") if t.strip()]    # sab-dom
RSS_URL        = os.environ.get("RSS_URL", "https://bitcoinblock.com.br/feed/")
AUTO_SKIP_CATS = {c.strip().lower() for c in os.environ.get("AUTO_SKIP_CATEGORIES", "Imprensa").split(",") if c.strip()}
AUTO_WAIT_MIN  = int(os.environ.get("AUTO_WAIT_MIN", "10"))     # espera a noticia do horario aparecer no RSS
AUTO_MAX_AGE_H = float(os.environ.get("AUTO_MAX_AGE_H", "24"))  # plano B: nunca manda noticia mais velha que isso
AUTO_FRESH_MIN = 60   # noticia "do horario" = publicada ate 60 min antes do horario
AUTO_GRACE_MIN = 30   # se o bot estava fora no horario, ainda envia ate 30 min depois

def fetch_rss():
    """Itens do RSS, mais novo primeiro: dicts com url, title, pub (UTC) e cats (minusculas).
    O parametro nc fura o cache do LiteSpeed (o feed tem max-age=3600)."""
    r = requests.get(RSS_URL, params={"nc": int(time.time())}, timeout=20,
                     headers={"User-Agent": "bbbot/2026-09 (+https://bitcoinblock.com.br)"})
    r.raise_for_status()
    root = ET.fromstring(r.content)
    items = []
    for it in root.iter("item"):
        link = (it.findtext("link") or "").strip()
        pub = it.findtext("pubDate")
        if not link or not pub or not _host_allowed(link):
            continue
        items.append({
            "url": link,
            "title": (it.findtext("title") or "").strip(),
            "pub": parsedate_to_datetime(pub.strip()).astimezone(timezone.utc),
            "cats": {(c.text or "").strip().lower() for c in it.findall("category")},
        })
    items.sort(key=lambda i: i["pub"], reverse=True)
    return items

def bootstrap_sent_urls(state):
    """1a vez desta versao: tudo que ja esta no RSS conta como enviado,
    para o plano B nunca reenviar artigo distribuido antes do deploy."""
    if "sent_urls" in state:
        return
    try:
        urls = [norm_url(i["url"]) for i in fetch_rss()]
    except Exception as e:
        log.warning("Bootstrap do RSS falhou (%s); tento no proximo boot", _safe(e))
        return
    state["sent_urls"] = urls[-MAX_SENT_URLS:]
    save_state(state)
    log.info("Bootstrap: %s links do RSS marcados como ja enviados", len(urls))

def due_slot(state, now_brt):
    """Horario de hoje que esta na hora e ainda nao foi feito: (chave 'HH:MM', datetime) ou (None, None)."""
    done = state.get("auto_done") or {}
    today = now_brt.strftime("%Y-%m-%d")
    done_today = done.get("slots", []) if done.get("day") == today else []
    times = AUTO_TIMES_WEEKEND if now_brt.weekday() >= 5 else AUTO_TIMES   # 5 = sabado, 6 = domingo
    for t in times:
        try:
            hh, mm = (int(x) for x in t.split(":"))
        except ValueError:
            continue
        slot = now_brt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if t not in done_today and slot <= now_brt <= slot + timedelta(minutes=AUTO_GRACE_MIN):
            return t, slot
    return None, None

def mark_slot(state, key, now_brt):
    today = now_brt.strftime("%Y-%m-%d")
    done = state.get("auto_done") or {}
    if done.get("day") != today:
        done = {"day": today, "slots": []}
    if key not in done["slots"]:
        done["slots"].append(key)
    state["auto_done"] = done
    save_state(state)

def post_to_source(item):
    """Posta o link no topico fonte (vira o registro do que saiu) e devolve a mensagem."""
    res = api_raw("sendMessage", {"chat_id": SOURCE_CHAT_ID, "message_thread_id": SOURCE_THREAD,
                                  "text": item["url"]}, http="post")
    if res.get("ok"):
        return res["result"]
    log.error("Nao consegui postar no topico fonte: %s %s", res.get("error_code"), res.get("description"))
    return None

def maybe_auto(state):
    """Roda a cada volta do loop: se for a hora de um horario, escolhe a noticia e distribui."""
    if not AUTO_ENABLED:
        return
    now = _now().astimezone(BRT)
    key, slot = due_slot(state, now)
    if not key:
        return
    if "sent_urls" not in state:    # bootstrap falhou no boot: sem historico, nao arrisca repetir
        bootstrap_sent_urls(state)
        if "sent_urls" not in state:
            return
    try:
        items = [i for i in fetch_rss() if not (i["cats"] & AUTO_SKIP_CATS)]
    except Exception as e:
        log.warning("RSS falhou no horario %s (%s); tento de novo", key, _safe(e))
        return                      # nova tentativa na proxima volta (dentro da tolerancia)
    sent = set(state.get("sent_urls", []))
    fresh = [i for i in items if i["pub"] >= slot - timedelta(minutes=AUTO_FRESH_MIN)]
    if fresh:
        pick = fresh[0]             # a ULTIMA noticia publicada
        if norm_url(pick["url"]) in sent:
            log.info("Horario %s: a noticia do horario ja foi distribuida (manual): %s", key, pick["url"])
            mark_slot(state, key, now)
            return
    elif now < slot + timedelta(minutes=AUTO_WAIT_MIN):
        return                      # a noticia do horario ainda nao apareceu no RSS; espera
    else:
        cutoff = _now() - timedelta(hours=AUTO_MAX_AGE_H)
        pick = next((i for i in items if i["pub"] >= cutoff and norm_url(i["url"]) not in sent), None)
        if not pick:
            log.info("Horario %s: nenhuma noticia nova no RSS; horario pulado", key)
            mark_slot(state, key, now)
            return
        log.info("Horario %s: nada publicado para este horario; enviando a mais nova ainda nao enviada", key)
    mark_slot(state, key, now)      # marca ANTES de postar: se cair no meio, nao duplica
    msg = post_to_source(pick)
    if not msg:
        notify_admin(f"⚠ Envio automatico das {key} falhou ao postar no topico fonte: {pick['url']}")
        return
    log.info("Horario %s: publicando '%s' %s", key, pick["title"][:80], pick["url"])
    distribute(msg, state)

# ----------------------------------------------------------------- Metadados (1x/dia)
def refresh_details():
    groups = load_groups()
    for g in groups:
        gid = g["group_id"]
        chat = api("getChat", {"chat_id": gid})
        if chat:
            g["title"] = chat.get("title", g.get("title"))
        cnt = api("getChatMemberCount", {"chat_id": gid})
        if isinstance(cnt, int):
            g["members_count"] = cnt
        for a in (api("getChatAdministrators", {"chat_id": gid}) or []):
            if a.get("status") == "creator":
                g["owner_username"] = a.get("user", {}).get("username", "Desconhecido")
                break
        time.sleep(0.3)
    save_groups(groups)
    log.info("Metadados de %s grupos atualizados", len(groups))

def maybe_monthly_report(state):
    """1 relatorio por mes para o chat admin; so marca como feito se entregou (senao tenta amanha)."""
    now = _now()
    tag = f"{now.year}-{now.month:02d}"
    if state.get("last_report") == tag:
        return
    groups = [g for g in load_groups() if g.get("active") is not False]
    if groups:
        lines = ["Relatorio mensal - grupos na rede:\n"]
        for g in groups:
            vinc = f" | BBDAO #{g['bbdao_user_id']}" if g.get("bbdao_user_id") else ""
            falha = f" | FALHANDO {g['fail_streak']}x" if g.get("fail_streak") else ""
            lines.append(f"- {g.get('title','?')} | membros: {g.get('members_count',0)} "
                         f"| dono: @{g.get('owner_username','?')}{vinc}{falha}")
        if not notify_admin("\n".join(lines)):
            return
    state["last_report"] = tag
    save_state(state)

# ----------------------------------------------------------------- Vinculo de parceiro (/vincular)
VINCULAR_HELP = (
    "Olá! \U0001F44B Sou o assistente do BitcoinBlock.com.br — levo notícias de "
    "blockchain selecionadas para grupos parceiros. Tem um grupo e quer conversar "
    "sobre uma parceria? Fale com a gente: aviso@bbdao.digital"
)

def parse_bbdao_id(arg):
    """Extrai o ID numerico da conta BBDAO: aceita o numero puro (123) ou o
    link de referencia inteiro (.../?r=123)."""
    if not arg:
        return None
    m = re.search(r"[?&]r=(\d+)", arg)
    if m:
        return int(m.group(1))
    m = re.fullmatch(r"\d{1,12}", arg.strip())
    return int(m.group(0)) if m else None

def is_group_admin(chat_id, user_id):
    """True se o usuario for criador/administrador do grupo (evita que um membro
    qualquer vincule o grupo a propria conta)."""
    if not user_id:
        return False
    res = api("getChatMember", {"chat_id": chat_id, "user_id": user_id})
    return bool(res) and res.get("status") in ("creator", "administrator")

def bbdao_link(group_id, uid, title, members, owner, linked_by):
    """Confirma o vinculo na plataforma BBDAO (POST /partners/link, X-API-Key)."""
    if not (BBDAO_API_URL and BBDAO_API_KEY):
        return False, "BBDAO_API_URL/BBDAO_API_KEY nao configurados"
    try:
        r = requests.post(
            f"{BBDAO_API_URL}/partners/link",
            headers={"X-API-Key": BBDAO_API_KEY, "Content-Type": "application/json"},
            json={
                "telegram_group_id": group_id,
                "bbdao_user_id": uid,
                "group_title": title,
                "members_count": members,
                "telegram_owner_username": owner,
                "linked_by": linked_by,
            },
            timeout=20,
        )
        data = r.json()
        return bool(data.get("success")), data.get("message", "")
    except Exception as e:
        return False, _safe(e)

def handle_vincular(msg, arg):
    chat = msg.get("chat", {})
    cid  = chat.get("id")
    if chat.get("type") not in ("group", "supergroup"):
        api("sendMessage", {"chat_id": cid, "text":
            "Use o /vincular DENTRO do seu grupo, onde o bot e administrador."}, http="post")
        return
    if cid == SOURCE_CHAT_ID:
        return
    uid = parse_bbdao_id(arg)
    frm = msg.get("from", {})
    log.info("/vincular chat=%s user=%s arg=%r", cid, frm.get("id"), (arg or "")[:40])
    if not uid:
        api("sendMessage", {"chat_id": cid, "text":
            "Uso: /vincular SEU_ID_BBDAO\n(o numero do seu link de referencia em "
            "bbdao.digital -> Referencias, ex.: /vincular 123)."}, http="post")
        return
    if not is_group_admin(cid, frm.get("id")):
        api("sendMessage", {"chat_id": cid, "text":
            "So um administrador/dono do grupo pode vincular. Peca pro dono enviar o /vincular."}, http="post")
        return
    register_group(chat)   # idempotente: garante o grupo na rede
    groups = load_groups()
    rec = next((g for g in groups if g["group_id"] == cid), None)
    linked_by = frm.get("username") or str(frm.get("id"))
    title   = (rec or {}).get("title") or chat.get("title", "")
    members = (rec or {}).get("members_count", 0)
    owner   = (rec or {}).get("owner_username", "")
    if rec is not None:
        rec["bbdao_user_id"] = uid
        rec["linked_by"]     = linked_by
        rec["linked_at"]     = _now().isoformat()
        save_groups(groups)
    ok, info = bbdao_link(cid, uid, title, members, owner, linked_by)
    log.info("/vincular '%s' (%s) -> BBDAO #%s: %s %s", title, cid, uid, "ok" if ok else "PENDENTE", info)
    if ok:
        # Confirmacao GENERICA no grupo (nao revela airdrop nem o ID da conta aos membros).
        api("sendMessage", {"chat_id": cid, "text":
            "✅ Parceria confirmada! Tudo certo do nosso lado."}, http="post")
        notify_admin(f"LINK ok: '{title}' ({cid}) -> conta BBDAO #{uid} (por @{linked_by})")
    else:
        api("sendMessage", {"chat_id": cid, "text":
            "Recebido! Registramos seu pedido; finalizamos do nosso lado em instantes."},
            http="post")
        notify_admin(f"LINK pendente (salvo local): '{title}' ({cid}) -> BBDAO #{uid} | motivo: {info}")

def handle_command(msg, text):
    """Trata /vincular e /start. Devolve True se o comando foi reconhecido."""
    parts = text.split()
    cmd = parts[0].split("@")[0].lower()
    arg = parts[1] if len(parts) > 1 else ""
    if cmd in ("/vincular", "/vinculargrupo"):
        handle_vincular(msg, arg)
        return True
    if cmd == "/start":
        api("sendMessage", {"chat_id": msg.get("chat", {}).get("id"), "text": VINCULAR_HELP}, http="post")
        return True
    return False

# ----------------------------------------------------------------- Handler de updates
def handle(update, state):
    # 1) Bot adicionado/removido de um grupo -> auto-registro
    if "my_chat_member" in update:
        ev = update["my_chat_member"]
        chat = ev["chat"]
        status = ev["new_chat_member"]["status"]
        if chat.get("type") in ("group", "supergroup"):
            if status in ("member", "administrator"):
                register_group(chat)
            elif status in ("left", "kicked"):
                unregister_group(chat["id"], f"(status={status})")
        return

    msg = update.get("message")
    if not msg:
        return

    # 1.5) Comandos (/vincular, /start) -> funcionam em qualquer chat
    text = (msg.get("text") or "").strip()
    if text.startswith("/") and handle_command(msg, text):
        return

    # 2) Mensagem no topico fonte -> distribuir
    if msg.get("chat", {}).get("id") != SOURCE_CHAT_ID:
        return
    if msg.get("message_thread_id") != SOURCE_THREAD:
        log.debug("Msg no grupo fonte mas em outro topico (thread real=%s, esperado=%s)",
                  msg.get("message_thread_id"), SOURCE_THREAD)
        return
    mid = msg["message_id"]
    if mid in state.get("seen", []):
        return
    if is_distributable(msg):
        distribute(msg, state)
    else:
        log.info("Mensagem %s ignorada no topico fonte (sem link de %s)", mid, ALLOWED_DOMAINS)

# ----------------------------------------------------------------- Boot / loop
def drain_backlog(state):
    """No 1o start, pula tudo que estava na fila p/ nao re-disparar links velhos."""
    res = api_raw("getUpdates", {"offset": -1, "timeout": 0})
    updates = res.get("result") or []
    state["offset"] = (updates[-1]["update_id"] + 1) if updates else 0
    save_state(state)
    log.info("Backlog ignorado. offset inicial = %s", state["offset"])

def poll_updates(state):
    """Long polling. 401/404 (token revogado/errado) derruba o processo com mensagem clara;
    outros erros esperam um pouco em vez de girar sem parar."""
    res = api_raw("getUpdates", {
        "offset": state["offset"],
        "timeout": 50,
        "allowed_updates": json.dumps(["message", "my_chat_member"]),
    }, timeout=70)
    if not res.get("ok"):
        code, desc = res.get("error_code"), res.get("description")
        log.error("getUpdates falhou: %s %s", code, desc)
        if code in (401, 404):
            raise SystemExit(f"getUpdates {code}: corrija TELEGRAM_TOKEN no Coolify e faca redeploy")
        time.sleep(30 if code == 409 else 5)   # 409 = outro getUpdates/webhook com o mesmo token
        return []
    return res.get("result") or []

def main():
    seed_groups_if_needed()
    state = load_state()
    if state.get("offset") is None:
        drain_backlog(state)
    if AUTO_ENABLED:
        bootstrap_sent_urls(state)
    log.info("Bot iniciado. fonte=%s topico=%s | %s grupos",
             SOURCE_CHAT_ID, SOURCE_THREAD, len(load_groups()))
    log.info("Envio automatico: %s | seg-sex %s | sab-dom %s (Brasilia) | ignora categorias %s",
             "LIGADO" if AUTO_ENABLED else "desligado", ", ".join(AUTO_TIMES) or "nenhum",
             ", ".join(AUTO_TIMES_WEEKEND) or "nenhum", sorted(AUTO_SKIP_CATS))

    while True:
        # tarefas diarias (metadados + relatorio mensal)
        if time.time() - state.get("last_details", 0) > DETAILS_REFRESH_S:
            try:
                refresh_details()
            except Exception:
                log.exception("Erro na atualizacao diaria")
            try:
                maybe_monthly_report(state)
            except Exception:
                log.exception("Erro no relatorio mensal")
            state["last_details"] = time.time()
            save_state(state)

        # envio automatico nos horarios (seg-sex 10:05, 14:05, 18:05 | sab-dom 12:05)
        try:
            maybe_auto(state)
        except Exception:
            log.exception("Erro no envio automatico")

        for u in poll_updates(state):
            state["offset"] = u["update_id"] + 1
            try:
                handle(u, state)
            except Exception:
                log.exception("Erro ao processar update %s", u.get("update_id"))
            save_state(state)

if __name__ == "__main__":
    main()
