#!/usr/bin/env python3
"""
Bitcoin Block - Bot de distribuicao de noticias para grupos do Telegram.
@bitcoin_block_bot

Fluxo:
  1. Dono de um grupo adiciona o bot e da permissao de postar.
     -> O bot se auto-registra (handler my_chat_member).
  2. Voce posta um link/imagem no topico "Artigos | Distribuicao" do grupo fonte.
     -> O bot COPIA a mensagem (link + imagem + legenda) para TODOS os grupos.
  3. O dono roda  /vincular SEU_ID  dentro do grupo.
     -> O bot liga o grupo a conta BBDAO do dono (airdrop de parceiros).

Robustez (o que faltava na versao antiga):
  - Estado persistido em disco (offset + mensagens ja enviadas) => sem spam ao reiniciar.
  - Backlog ignorado no 1o boot => nao re-dispara links velhos.
  - Retry/backoff em erro de rede + tratamento de 429 (rate limit).
  - Remove automaticamente grupos onde o bot foi expulso/bloqueado (403).
  - Auto-registro de novos grupos (handler my_chat_member).
  - Atualiza titulos/membros 1x/dia (nao a cada 10s).
"""

import os
import re
import json
import time
import logging
from datetime import datetime, timezone
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
log = logging.getLogger("bbbot")

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
            log.warning("Rede falhou em %s (%s); retry em %ss", method, e, wait)
            time.sleep(wait)
    return {"ok": False, "error_code": -1, "description": "falha de rede"}

def api(method, params=None, http="get", timeout=30):
    return api_raw(method, params, http, timeout).get("result")

def notify_admin(text):
    if ADMIN_CHAT_ID:
        api("sendMessage", {"chat_id": ADMIN_CHAT_ID, "text": text}, http="post")

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

# ----------------------------------------------------------------- Distribuicao
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

def is_distributable(msg):
    """So distribui se a mensagem tiver um link de um dominio permitido.
    Imagem/print sozinho (sem link permitido) NAO e distribuido."""
    text = msg.get("text") or msg.get("caption") or ""
    return any(_host_allowed(link) for link in LINK_RE.findall(text))

def broadcast(from_chat_id, message_id):
    groups = load_groups()
    sent = 0
    for g in list(groups):
        gid = g["group_id"]
        if gid == SOURCE_CHAT_ID:
            continue              # nunca ecoa de volta no grupo fonte
        params = {"chat_id": gid, "from_chat_id": from_chat_id, "message_id": message_id}
        if g.get("has_topics") and g.get("selected_thread_id"):
            params["message_thread_id"] = g["selected_thread_id"]
        res = api_raw("copyMessage", params, http="post")
        if res.get("ok"):
            sent += 1
        else:
            code = res.get("error_code")
            desc = (res.get("description") or "").lower()
            if code == 403 or any(k in desc for k in ("kicked", "blocked", "not a member", "chat not found")):
                unregister_group(gid, f"(403: {desc})")
            else:
                log.warning("Falha ao enviar p/ %s: %s %s", gid, code, desc)
        time.sleep(SEND_DELAY)
    log.info("Mensagem %s distribuida para %s/%s grupos", message_id, sent, len(groups))

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
    now = datetime.now(timezone.utc)
    tag = f"{now.year}-{now.month:02d}"
    if now.day == 1 and state.get("last_report") != tag:
        groups = load_groups()
        if groups:
            lines = ["Relatorio mensal - grupos na rede:\n"]
            for g in groups:
                vinc = f" | BBDAO #{g['bbdao_user_id']}" if g.get("bbdao_user_id") else ""
                lines.append(f"- {g.get('title','?')} | membros: {g.get('members_count',0)} "
                             f"| dono: @{g.get('owner_username','?')}{vinc}")
            notify_admin("\n".join(lines))
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
        return False, str(e)

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
    if not uid:
        api("sendMessage", {"chat_id": cid, "text":
            "Uso: /vincular SEU_ID_BBDAO\n(o numero do seu link de referencia em "
            "bbdao.digital -> Referencias, ex.: /vincular 123)."}, http="post")
        return
    frm = msg.get("from", {})
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
        rec["linked_at"]     = datetime.now(timezone.utc).isoformat()
        save_groups(groups)
    ok, info = bbdao_link(cid, uid, title, members, owner, linked_by)
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
        log.info("Msg no grupo fonte mas em outro topico (thread real=%s, esperado=%s)",
                 msg.get("message_thread_id"), SOURCE_THREAD)
        return
    mid = msg["message_id"]
    if mid in state["seen"]:
        return
    if is_distributable(msg):
        broadcast(SOURCE_CHAT_ID, mid)
        state["seen"] = (state["seen"] + [mid])[-MAX_SEEN:]
        save_state(state)
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

def main():
    seed_groups_if_needed()
    state = load_state()
    if state.get("offset") is None:
        drain_backlog(state)
    log.info("Bot iniciado. fonte=%s topico=%s | %s grupos",
             SOURCE_CHAT_ID, SOURCE_THREAD, len(load_groups()))

    while True:
        # tarefas diarias (metadados + relatorio do dia 1)
        if time.time() - state.get("last_details", 0) > DETAILS_REFRESH_S:
            try:
                refresh_details()
                maybe_monthly_report(state)
            except Exception:
                log.exception("Erro nas tarefas diarias")
            state["last_details"] = time.time()
            save_state(state)

        updates = api("getUpdates", {
            "offset": state["offset"],
            "timeout": 50,
            "allowed_updates": json.dumps(["message", "my_chat_member"]),
        }, timeout=70) or []

        for u in updates:
            state["offset"] = u["update_id"] + 1
            try:
                handle(u, state)
            except Exception:
                log.exception("Erro ao processar update %s", u.get("update_id"))
            save_state(state)

if __name__ == "__main__":
    main()
