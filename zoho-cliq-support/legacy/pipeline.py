from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect
import requests
import os
import json
from datetime import datetime, timezone
import uuid

from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    PointStruct,
    PayloadSchemaType,
    Filter,
    FieldCondition,
    MatchValue,
)
from google import genai

app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # change for prod

# ========= OAuth for Cliq (optional, unchanged) =========
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/oauth/callback'
AUTH_URL = 'https://accounts.zoho.com/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.com/oauth/v2/token'
SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

ACCESS_TOKEN = None
REFRESH_TOKEN = None

# ========= Signals =========
SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')

# ========= Catalyst Data Store =========
CATALYST_TOKEN = os.getenv("CATALYST_TOKEN")          # Zoho-oauthtoken with DataStore.row.CREATE,DataStore.row.READ
CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
CONVERSATIONS_TABLE = "conversations"                # must exist with matching columns

# ========= Qdrant / Gemini =========
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = "messages_vec"

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
genai_client = genai.Client()   # uses GEMINI_API_KEY env

BOT_NAME = "workspace-vita"  # lowercased for checks


# ---------- Embedding + Qdrant helpers ----------

def embed_text(text: str) -> list[float]:
    res = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )
    return res.embeddings[0].values


def ensure_qdrant_collection(vector_dim: int):
    collections = qdrant.get_collections().collections
    if not any(c.name == QDRANT_COLLECTION for c in collections):
        qdrant.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
        )
        print(f"✅ Created Qdrant collection {QDRANT_COLLECTION}")
    try:
        qdrant.create_payload_index(
            collection_name=QDRANT_COLLECTION,
            field_name="category",
            field_schema=PayloadSchemaType.KEYWORD,
        )
    except Exception:
        pass  # already indexed


def classify_message(text: str) -> str:
    t = text.lower()
    if any(w in t for w in ["error", "down", "timeout", "issue", "bug", "problem", "fail"]):
        return "incident"
    if any(w in t for w in ["fixed", "resolved", "restarted", "deployed", "solved", "restored"]):
        return "response"
    return "discussion"


def normalize_message_id(raw_id: str) -> str:
    """Convert Cliq message_id (with %20 etc.) into a stable UUID for Qdrant."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


# ---------- Data Store helpers ----------

def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
    """Insert one row into conversations table. time_stamp is bigint."""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        print("⚠️ Catalyst config missing; skipping Data Store insert")
        return None

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    body = [{
        "conversation_id": conversation_id,
        "message_id": message_id,
        "sender_id": sender_id,
        "time_stamp": int(timestamp_ms),  # column type: bigint
        "message_text": message_text,
        "category": category,
    }]

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        print("DS insert status:", resp.status_code, resp.text)
        if resp.status_code == 201:
            return resp.json()[0].get("ROWID")
    except Exception as e:
        print("DS insert exception:", e)
    return None


def fetch_by_message_ids(message_ids: list[str]):
    """Fetch messages from Data Store and filter by message_id."""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID and message_ids):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        resp = requests.get(f"{url}?max_rows=200", headers=headers, timeout=10)
        if resp.status_code != 200:
            print("DS fetch error:", resp.status_code, resp.text)
            return []
        rows = resp.json().get("data", [])
        return [r for r in rows if r.get("message_id") in message_ids]
    except Exception as e:
        print("DS fetch exception:", e)
        return []


def fetch_recent_incidents(limit: int = 5):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.in/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        resp = requests.get(f"{url}?max_rows=200", headers=headers, timeout=10)
        if resp.status_code != 200:
            print("DS fetch error:", resp.status_code, resp.text)
            return []
        rows = resp.json().get("data", [])
        incidents = [r for r in rows if r.get("category") == "incident"]
        incidents.sort(key=lambda r: r.get("time_stamp", 0), reverse=True)
        return incidents[:limit]
    except Exception as e:
        print("Recent incidents fetch exception:", e)
        return []


# ---------- Full indexing pipeline for normal messages ----------

def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
    """Classify, store in Data Store, then embed & index in Qdrant."""
    category = classify_message(message_text)
    print(f"📋 Category: {category}")

    row_id = insert_into_datastore(
        conversation_id, message_id, sender_id, timestamp_ms, message_text, category
    )

    emb = embed_text(message_text)
    ensure_qdrant_collection(len(emb))

    qdrant_id = normalize_message_id(message_id)

    point = PointStruct(
        id=qdrant_id,
        vector=emb,
        payload={
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "category": category,
            "row_id": row_id,
            "message_id": message_id,  # keep original id
        },
    )
    qdrant.upsert(QDRANT_COLLECTION, [point])
    print(f"✅ Indexed in Qdrant: cli_id={message_id}, qdrant_id={qdrant_id}")


# ---------- Command handlers ----------

def handle_search_command(query: str, chat: dict, user: dict):
    print(f"Running semantic search for query: {query!r}")
    # 1) Embed query
    q_emb = embed_text(query)

    # 2) Filter only incidents
    q_filter = Filter(
        must=[FieldCondition(key="category", match=MatchValue(value="incident"))]
    )

    # 3) Search Qdrant
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=5,
    )

    print("Qdrant hits:", hits)

    if not hits:
        return jsonify({"text": f"No similar incidents found for `{query}`."})

    # 4) Collect original message_ids from payload
    message_ids = [
        h.payload.get("message_id")
        for h in hits
        if h.payload and h.payload.get("message_id")
    ]
    if not message_ids:
        return jsonify({"text": f"No similar incidents found for `{query}`."})

    # 5) Hydrate from Data Store
    rows = fetch_by_message_ids(message_ids)

    lines = [f"*Past incidents similar to:* `{query}`"]
    for h in hits:
        mid = h.payload.get("message_id") if h.payload else None
        if not mid:
            continue
        row = next((r for r in rows if r.get("message_id") == mid), None)
        if not row:
            continue
        txt = row.get("message_text") or ""
        snippet = (txt[:120] + "…") if len(txt) > 120 else txt
        cat = row.get("category")
        lines.append(f"- ({h.score:.2f}) [{cat}] {snippet}")

    return jsonify({"text": "\n".join(lines)})


# def handle_search_command(query: str, chat: dict, user: dict):
#     print(f"Running semantic search for query: {query!r}")
#     q_emb = embed_text(query)

#     q_filter = Filter(
#         must=[FieldCondition(key="category", match=MatchValue(value="incident"))]
#     )

#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=q_emb,
#         query_filter=q_filter,
#         limit=5,
#     )

#     if not hits:
#         return jsonify({"text": f"No similar incidents found for `{query}`."})

#     message_ids = [h.payload.get("message_id") for h in hits if h.payload]
#     rows = fetch_by_message_ids(message_ids)

#     lines = [f"*Past incidents similar to:* `{query}`"]
#     for h in hits:
#         mid = h.payload.get("message_id") if h.payload else None
#         row = next((r for r in rows if r.get("message_id") == mid), None)
#         if not row:
#             continue
#         txt = row.get("message_text") or ""
#         snippet = (txt[:120] + "…") if len(txt) > 120 else txt
#         cat = row.get("category")
#         lines.append(f"- ({h.score:.2f}) [{cat}] {snippet}")

#     return jsonify({"text": "\n".join(lines)})


def handle_latest_issues(chat: dict, user: dict):
    rows = fetch_recent_incidents(limit=5)
    if not rows:
        return jsonify({"text": "No incidents recorded recently."})

    lines = ["*Latest incidents:*"]
    for r in rows:
        txt = r.get("message_text") or ""
        snippet = (txt[:120] + "…") if len(txt) > 120 else txt
        conv = r.get("conversation_id")
        ts = r.get("time_stamp")
        lines.append(f"- [{conv}] {snippet} (ts={ts})")

    return jsonify({"text": "\n".join(lines)})


def handle_issue_details(incident_id: str, chat: dict, user: dict):
    # Placeholder: you can implement later using incident_id column
    return jsonify({"text": f"Details for incident `{incident_id}` are not implemented yet."})


# ---------- /bot/command endpoint ----------

@app.route('/bot/command', methods=['POST'])
def bot_command():
    form_data = request.form.to_dict()
    print("==== /bot/command hit ====")
    print(form_data)

    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    command_text = form_data.get('commandText', '').strip()

    try:
        user = json.loads(user_raw)
        chat = json.loads(chat_raw)
    except Exception as e:
        print("Error parsing user/chat in command:", e)
        return jsonify({"text": "Sorry, I couldn't read that command."})

    tokens = command_text.split()
    if not tokens:
        return jsonify({"text": "Usage: search <query> or latest_issues"})

    cmd = tokens[0].lower()
    args = " ".join(tokens[1:]).strip()

    if cmd == "search":
        if not args:
            return jsonify({"text": "Usage: search <query>"})
        return handle_search_command(args, chat, user)

    if cmd in ("latest_issues", "issues"):
        return handle_latest_issues(chat, user)

    if cmd == "issue" and args:
        return handle_issue_details(args, chat, user)

    return jsonify({"text": f"Unknown command `{cmd}`. Try: search, latest_issues."})


# ========= OAuth endpoints (unchanged) =========

@app.route('/login')
def login():
    params = {
        'client_id': CLIENT_ID,
        'scope': SCOPE,
        'response_type': 'code',
        'redirect_uri': REDIRECT_URI,
        'access_type': 'offline',
        'prompt': 'consent'
    }
    auth_request = requests.Request('GET', AUTH_URL, params=params).prepare()
    return redirect(auth_request.url)


@app.route('/oauth/callback')
def oauth_callback():
    global ACCESS_TOKEN, REFRESH_TOKEN
    code = request.args.get('code')
    if not code:
        return "Missing authorization code", 400

    data = {
        'code': code,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'redirect_uri': REDIRECT_URI,
        'grant_type': 'authorization_code'
    }
    resp = requests.post(TOKEN_URL, data=data)
    token_json = resp.json()

    if 'access_token' not in token_json:
        return f"Failed to get access token: {token_json}", 400

    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful!"


# ========= Producer: Deluge → Flask → Signals =========

@app.route('/bot/events', methods=['POST'])
def bot_events():
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    print(form_data)

    operation = form_data.get('operation')
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    data_raw = form_data.get('data')

    inner_data = {
        "source": "zoho_cliq_bot",
        "operation": operation,
        "user": user_raw,
        "chat": chat_raw,
        "raw": data_raw
    }

    event_payload = {"data": inner_data}

    if SIGNALS_EVENT_URL:
        try:
            resp = requests.post(
                SIGNALS_EVENT_URL,
                headers={"Content-Type": "application/json"},
                json=event_payload
            )
            print("Signals response:", resp.status_code, resp.text)
        except Exception as e:
            print("Error publishing to Signals:", e)

    return jsonify({"status": "ok"})


# ========= Consumer: Signals → Flask → Data Store + Qdrant =========

@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    print(json.dumps(payload, indent=2))

    events = payload.get("events", [])
    if not events:
        return jsonify({"status": "no_events"}), 200

    event_obj = events[0]
    outer_data = event_obj.get("data", {})
    inner_data = outer_data.get("data", {})

    raw_str = inner_data.get("raw")
    user_str = inner_data.get("user")
    chat_str = inner_data.get("chat")

    if not (raw_str and user_str and chat_str):
        print("Missing raw/user/chat; skipping")
        return jsonify({"status": "ignored"}), 200

    try:
        raw = json.loads(raw_str)
        user = json.loads(user_str)
        chat = json.loads(chat_str)
    except Exception as e:
        print("Error parsing JSON fields:", e)
        return jsonify({"status": "parse_error"}), 200

    try:
        message_text = raw["message"]["content"]["text"]
        message_id = raw["message"]["id"]
        timestamp_ms = raw["time"]
    except KeyError as e:
        print("Missing field in raw:", e)
        return jsonify({"status": "bad_raw"}), 200

    # Skip bot command messages from indexing
    if f"@{BOT_NAME}" in message_text.lower():
        print("Skipping indexing for bot command message")
        return jsonify({"status": "command_skipped"}), 200

    sender_id = user.get("zoho_user_id") or user.get("id")
    conversation_id = chat.get("id")  # or channel_unique_name / channel_id

    print(f"📨 {message_text!r}")
    print(f"   conv={conversation_id}, sender={sender_id}, ts_ms={timestamp_ms}")

    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)

    return jsonify({"status": "processed"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
