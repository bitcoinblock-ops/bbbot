#!/usr/bin/env python3
"""
Bitcoin Block - Bot de distribuicao de noticias para grupos do Telegram.
@bitcoin_block_bot

Fluxo:
  1. Dono de um grupo adiciona o bot e da permissao de postar.
     -> O bot se auto-registra (handler my_chat_member).
  2. Voce posta um link/imagem no topico "Artigos | Distribuicao" do grupo fonte.
     -> O bot COPIA a mensagem (link + imagem + legenda) para TODOS os grupos.

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

SEND_DELAY        = 1.0           # s entre envios (folga no limite global ~30/s)
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
    """No 1o boot (volume /data vazio) inicializa a lista de grupos.
    Ordem: variavel SEED_GROUPS_JSON -> arquivo seed -> vazio."""
    if os.path.exists(GROUPS_FILE):
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
    groups = load_groups()
    gid = chat["id"]
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
        "text": ("Bot Bitcoin Block conectado a este grupo. A partir de agora "
                 "voce recebe noticias selecionadas de blockchain (sem spam). "
                 "Confirme que o bot tem permissao de enviar mensagens."),
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

def is_distributable(msg):
    """So distribui mensagens do topico fonte que tenham link OU imagem."""
    if "photo" in msg:
        return True
    text = msg.get("text") or msg.get("caption") or ""
    return bool(LINK_RE.search(text))

def broadcast(from_chat_id, message_id):
    groups = load_groups()
    sent = 0
    for g in list(groups):
        gid = g["group_id"]
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
                lines.append(f"- {g.get('title','?')} | membros: {g.get('members_count',0)} "
                             f"| dono: @{g.get('owner_username','?')}")
            notify_admin("\n".join(lines))
        state["last_report"] = tag
        save_state(state)

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

    # 2) Mensagem no topico fonte -> distribuir
    msg = update.get("message")
    if not msg:
        return
    if msg.get("chat", {}).get("id") != SOURCE_CHAT_ID:
        return
    if msg.get("message_thread_id") != SOURCE_THREAD:
        return
    mid = msg["message_id"]
    if mid in state["seen"]:
        return
    if is_distributable(msg):
        broadcast(SOURCE_CHAT_ID, mid)
        state["seen"] = (state["seen"] + [mid])[-MAX_SEEN:]
        save_state(state)

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
