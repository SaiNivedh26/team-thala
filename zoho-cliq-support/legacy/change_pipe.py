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
app.secret_key = 'YOUR_SECRET_KEY'

# ========= OAuth for Cliq =========
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
CATALYST_TOKEN = os.getenv("CATALYST_TOKEN")
CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
CONVERSATIONS_TABLE = "conversations"  # messages table
ISSUES_TABLE = "issues"  # issues table (you must create this)

# ========= Qdrant / Gemini =========
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = "messages_vec"

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
genai_client = genai.Client()

BOT_NAME = "workspace-vita"

# ---------- LLM Classification ----------
def classify_message_llm(text: str) -> dict:
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    prompt = f"""Classify this engineering message:

Statement: "{text}"

Return ONLY this JSON (no extra text):
{{"role": "incident|discussion|resolution", "category": "database|cache|auth|network|security|deployment|other", "severity": "low|medium|high"}}

Rules:
- role:
  - "resolution" ONLY if the message explicitly states that a problem is ALREADY fixed, resolved, closed, working now, back to normal, or issue is gone
  - "incident" if it reports an active problem, failure, error, outage, or something broken
  - "discussion" for everything else
- category: database, cache, auth, network, security, deployment, or other
- severity: low, medium, or high

JSON:"""

    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Return ONLY valid JSON. Be strict about 'resolution'.",
        "temperature": 0.1,
        "max_tokens": 128,
    }

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        if resp.status_code != 200:
            return {"role": "discussion", "category": "other", "severity": "low"}
        
        result = resp.json()
        output_text = result.get("data", {}).get("output_text") or result.get("response", "")
        
        if "{" in output_text and "}" in output_text:
            start = output_text.index("{")
            end = output_text.rindex("}") + 1
            json_str = output_text[start:end]
        else:
            json_str = output_text.strip()
            
        classification = json.loads(json_str)
        print(f"✅ Parsed classification: {classification}")
        return classification
        
    except Exception as e:
        print(f"LLM classification error: {e}")
        return {"role": "discussion", "category": "other", "severity": "low"}

def get_latest_open_issue_for_conversation(conversation_id: str):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return None

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {"Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}", "CATALYST-ORG": CATALYST_ORG_ID}

    try:
        resp = requests.get(f"{url}?max_rows=300", headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        rows = resp.json().get("data", [])
        open_rows = [r for r in rows if isinstance(r.get("status"), str) and r["status"].strip().lower() == "open"]
        if not open_rows:
            return None
        open_rows.sort(key=lambda r: int(r.get("opened_at", 0)), reverse=True)
        return open_rows[0]
    except Exception as e:
        print("get_latest_open_issue_for_conversation exception:", e)
        return None

# ---------- Embedding + Qdrant ----------
def embed_text(text: str) -> list[float]:
    res = genai_client.models.embed_content(model="gemini-embedding-001", contents=text)
    return res.embeddings[0].values

def ensure_qdrant_collection(vector_dim: int):
    collections = qdrant.get_collections().collections
    if not any(c.name == QDRANT_COLLECTION for c in collections):
        qdrant.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
        )
        print(f"✅ Created Qdrant collection {QDRANT_COLLECTION}")
    
    for field in ["role", "category", "issue_id"]:
        try:
            qdrant.create_payload_index(
                collection_name=QDRANT_COLLECTION,
                field_name=field,
                field_schema=PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass

def normalize_message_id(raw_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))

# ---------- Data Store: Messages ----------
def insert_message_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, 
                                  message_text, role, category, severity, issue_id):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
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
        "time_stamp": int(timestamp_ms),
        "message_text": message_text,
        "role": role,
        "category": category,
        "severity": severity,
        "issue_id": issue_id or "",
    }]

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        if resp.status_code == 201:
            return resp.json()[0].get("ROWID")
    except Exception:
        pass
    return None

def fetch_messages_by_issue_id(issue_id: str):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {"Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}", "CATALYST-ORG": CATALYST_ORG_ID}

    all_rows = []
    next_token = None

    try:
        while True:
            params = f"max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"
            
            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            if resp.status_code != 200:
                break
            
            body = resp.json()
            rows = body.get("data", [])
            matching_rows = [r for r in rows if r.get("issue_id") == issue_id]
            all_rows.extend(matching_rows)
            
            next_token = body.get("next_token")
            if not next_token:
                break
        
        return all_rows
        
    except Exception:
        return []

# ---------- Data Store: Issues ----------
def create_issue_in_datastore(issue_id, title, source, category, severity, opened_at):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return False

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    body = [{
        "issue_id": issue_id,
        "title": title,
        "source": source,
        "category": category,
        "severity": severity,
        "status": "Open",
        "opened_at": int(opened_at),
        "resolved_at": 0,
    }]

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        return resp.status_code == 201
    except Exception:
        return False

def close_issue_in_datastore(issue_id: str, resolved_at_ms: int) -> bool:
    base_url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        get_resp = requests.get(f"{base_url}?max_rows=300", headers=headers, timeout=10)
        if get_resp.status_code != 200:
            return False

        rows = get_resp.json().get("data", [])
        issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
        if not issue_row:
            return False

        row_id = issue_row.get("ROWID")
        if not row_id:
            return False

        update_body = [{
            "ROWID": row_id,
            "status": "Resolved",
            "resolved_at": str(int(resolved_at_ms)),
        }]

        put_resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
        return put_resp.status_code == 200

    except Exception:
        return False

def store_resolution_summary(issue_id: str, summary: str, resolved_at_ms: int) -> bool:
    """Append resolution to title (no schema change needed)"""
    base_url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        get_resp = requests.get(f"{base_url}?max_rows=300", headers=headers, timeout=10)
        if get_resp.status_code != 200:
            return False

        rows = get_resp.json().get("data", [])
        issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
        if not issue_row:
            return False

        row_id = issue_row.get("ROWID")
        current_title = issue_row.get("title", "")
        new_title = f"{current_title} | RESOLVED: {summary[:100]}"
        
        update_body = [{
            "ROWID": row_id,
            "status": "Resolved",
            "resolved_at": str(int(resolved_at_ms)),
            "title": new_title,
        }]

        put_resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
        print(f"DS store resolution: {put_resp.status_code}")
        return put_resp.status_code == 200

    except Exception as e:
        print(f"Store resolution exception: {e}")
        return False

def fetch_open_issues():
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {"Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}", "CATALYST-ORG": CATALYST_ORG_ID}

    all_rows = []
    next_token = None

    try:
        while True:
            params = "max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"

            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            if resp.status_code != 200:
                break

            body = resp.json()
            rows = body.get("data", [])
            all_rows.extend(rows)

            next_token = body.get("next_token")
            if not next_token:
                break

        open_issues_list = [r for r in all_rows if isinstance(r.get("status"), str) and r["status"].strip().lower() == "open"]
        open_issues_list.sort(key=lambda r: int(r.get("opened_at", 0)), reverse=True)
        return open_issues_list

    except Exception:
        return []

def fetch_all_issues():
    """Fetch ALL issues (Open + Resolved), sorted by recency"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {"Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}", "CATALYST-ORG": CATALYST_ORG_ID}

    all_rows = []
    next_token = None

    try:
        while True:
            params = "max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"

            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            if resp.status_code != 200:
                break

            body = resp.json()
            rows = body.get("data", [])
            all_rows.extend(rows)

            next_token = body.get("next_token")
            if not next_token:
                break

        def sort_key(issue):
            resolved = int(issue.get("resolved_at", 0))
            opened = int(issue.get("opened_at", 0))
            return max(resolved, opened)

        all_rows.sort(key=sort_key, reverse=True)
        return all_rows

    except Exception:
        return []

def summarize_resolution_with_llm(messages: list) -> str:
    if not messages:
        return "No resolution details available."
    
    relevant = [m for m in messages if m.get("role") in ["incident", "discussion", "resolution"]]
    if not relevant:
        return "Issue resolved (no details recorded)."
    
    incident = next((m.get("message_text", "") for m in relevant if m.get("role") == "incident"), "")
    discussion = "\n".join([m.get("message_text", "") for m in relevant if m.get("role") == "discussion"][:5])
    resolutions = [m.get("message_text", "") for m in relevant if m.get("role") == "resolution"]
    
    text_concat = f"Incident: {incident}\nDiscussion: {discussion}\nResolutions: {'; '.join(resolutions)}"
    
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {CATALYST_TOKEN}", "CATALYST-ORG": CATALYST_ORG_ID}
    
    prompt = f"""Summarize how this incident was resolved:

{text_concat}

Provide a concise 1-2 sentence summary of WHAT was done to fix it."""
    
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Summarize incident resolutions concisely.",
        "temperature": 0.3,
        "max_tokens": 150,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        if resp.status_code == 200:
            result = resp.json()
            return result.get("data", {}).get("output_text", "Issue resolved.").strip()
    except Exception:
        pass
    
    return "Issue resolved."

# ---------- Main Indexing Pipeline ----------
def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
    cls = classify_message_llm(message_text)
    role = cls.get("role", "discussion")
    category = cls.get("category", "other")
    severity = cls.get("severity", "low")
    
    print(f"📋 Role: {role}, Category: {category}, Severity: {severity}")
    
    emb = embed_text(message_text)
    ensure_qdrant_collection(len(emb))
    
    issue_id = None
    
    if role == "incident":
        existing_open = fetch_open_issues()
        normalized_title = message_text.strip().lower()
        for row in existing_open:
            if row.get("title", "").strip().lower() == normalized_title:
                issue_id = row.get("issue_id")
                print(f"🔗 Reusing existing open issue: {issue_id}")
                break

        if not issue_id:
            issue_id = str(uuid.uuid4())
            title = message_text[:100] + ("..." if len(message_text) > 100 else "")
            create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
            print(f"🆕 Created new issue: {issue_id}")

    elif role in ["discussion", "resolution"]:
        latest_open = get_latest_open_issue_for_conversation(conversation_id)
        if latest_open:
            issue_id = latest_open.get("issue_id")
            print(f"🔗 Linked {role} to latest open issue: {issue_id}")

        if role == "resolution" and issue_id:
            print(f"🔧 Processing resolution for issue: {issue_id}")
            messages = fetch_messages_by_issue_id(issue_id)
            summary = summarize_resolution_with_llm(messages)
            print(f"✍️ Generated summary: {summary}")
            
            success = store_resolution_summary(issue_id, summary, timestamp_ms)
            if success:
                print(f"✅ SUCCESS: Closed issue {issue_id} with summary")
            else:
                print(f"❌ FAILED: Could not close issue {issue_id}")

    row_id = insert_message_into_datastore(
        conversation_id, message_id, sender_id, timestamp_ms,
        message_text, role, category, severity, issue_id
    )
    
    qdrant_id = normalize_message_id(message_id)
    point = PointStruct(
        id=qdrant_id,
        vector=emb,
        payload={
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "role": role,
            "category": category,
            "severity": severity,
            "issue_id": issue_id or "",
            "row_id": row_id,
            "message_id": message_id,
        },
    )
    qdrant.upsert(QDRANT_COLLECTION, [point])
    print(f"✅ Indexed in Qdrant: {message_id}")

# ---------- Command Handlers ----------
def handle_search_command(query: str):
    """🔍 Search incidents command"""
    print(f"🔍 Searching incidents for: {query}")
    
    q_emb = embed_text(query)
    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=10,
    )
    
    if not hits:
        return jsonify({"text": f"No incidents found for `{query}`."})
    
    issue_scores = {}
    for hit in hits:
        if hit.payload and (iid := hit.payload.get("issue_id")):
            issue_scores.setdefault(iid, 0)
            issue_scores[iid] = max(issue_scores[iid], hit.score)
    
    all_issues = fetch_all_issues()
    scored_issues = []
    for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True):
        issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
        if issue:
            issue["search_score"] = score
            scored_issues.append(issue)
    
    if not scored_issues:
        return jsonify({"text": "No issue details found."})
    
    lines = [f"🔍 **Search Results for: `{query}`** 📋\n━━━━━━━━━━━━━━━━━━━━━━━\n"]
    
    for idx, issue in enumerate(scored_issues[:5], 1):
        issue_id = issue.get("issue_id", "")[:12] + "..."
        title = issue.get("title", "")[:80]
        category = issue.get("category", "other")
        severity = issue.get("severity", "low")
        status = issue.get("status", "Open")
        
        opened_ts = int(issue.get("opened_at", 0))
        opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_ts else "N/A"
        
        resolved_ts = int(issue.get("resolved_at", 0))
        resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_ts else "—"
        
        # Extract resolution from title
        resolution_summary = "🟡 Open (no resolution yet)"
        if "| RESOLVED:" in title:
            resolution_summary = title.split("| RESOLVED:")[-1].strip()
        elif resolved_ts > 0:
            resolution_summary = "✅ Resolved"
        
        lines.extend([
            f"**#{idx} {title}**  {severity.upper()}",
            f"🆔 `{issue_id}` | 📦 {category} | 📊 Score: {issue.get('search_score', 0):.2f}",
            f"📅 Opened: {opened_str} | ✅ {status} at: {resolved_str}",
            f"🔧 **Resolution:** {resolution_summary}",
            "━━━━━━━━━━━━━━━━━━━━━━━"
        ])
    
    return jsonify({"text": "\n".join(lines)})

@app.route('/latest_issues_json', methods=['GET'])
def latest_issues_json():
    issues = fetch_open_issues()
    out = []
    for issue in issues[:10]:
        out.append({
            "issue_id": issue.get("issue_id", ""),
            "title": issue.get("title", ""),
            "category": issue.get("category", "other"),
            "severity": issue.get("severity", "low"),
            "source": issue.get("source", "Cliq"),
            "opened_at": issue.get("opened_at", 0),
            "opened_at_str": datetime.fromtimestamp(int(issue.get("opened_at", 0))/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if issue.get("opened_at") else "N/A"
        })
    return jsonify({"issues": out})

@app.route('/issue_conversations_json', methods=['GET'])
def issue_conversations_json():
    issue_id = request.args.get("issue_id", "").strip()
    if not issue_id:
        return jsonify({"messages": []})

    messages = fetch_messages_by_issue_id(issue_id)
    if not messages:
        return jsonify({"text": f"No conversations for issue: {issue_id}"})

    try:
        messages.sort(key=lambda m: int(m.get("time_stamp", 0)))
    except Exception:
        pass

    out = []
    for m in messages:
        ts_raw = m.get("time_stamp", 0)
        ts_str = datetime.fromtimestamp(int(ts_raw)/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if ts_raw else "N/A"
        out.append({
            "time_str": ts_str,
            "role": m.get("role", "discussion"),
            "sender_id": m.get("sender_id", ""),
            "message_text": m.get("message_text", ""),
        })
    return jsonify({"messages": out})

# ---------- Bot Command Handler ----------
@app.route('/bot/command', methods=['POST'])
def bot_command():
    form_data = request.form.to_dict()
    
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    command_text = form_data.get('commandText', '').strip()

    try:
        user = json.loads(user_raw)
        chat = json.loads(chat_raw)
    except Exception:
        return jsonify({"text": "Error parsing command."})

    # Parse command: @workspace-vita search database down
    message_lower = command_text.lower()
    bot_mention = f"@{BOT_NAME.lower()}"
    
    if bot_mention in message_lower:
        # Extract command after bot mention
        after_bot = message_lower.split(bot_mention)[-1].strip()
        
        if after_bot.startswith("search "):
            query = after_bot[7:].strip()  # Remove "search "
            if query:
                print(f"🔍 Search command detected: {query}")
                return handle_search_command(query)
        
        elif after_bot in ["latest_issues", "issues"]:
            issues = fetch_open_issues()
            if not issues:
                return jsonify({"text": "No open issues at the moment."})
            
            txt = "📋 **Latest Open Issues**\n\n"
            for idx, issue in enumerate(issues[:5], 1):
                short_id = issue.get("issue_id", "")[:12] + "..."
                opened_at = datetime.fromtimestamp(int(issue.get("opened_at", 0))/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
                txt += f"**#{idx}. {issue.get('title', '')}**\n"
                txt += f"🆔 {short_id} | 📦 {issue.get('category', 'other')} | ⚠️ {issue.get('severity', 'low')}\n"
                txt += f"📅 {opened_at}\n━━━━━━━━━━━━━━━━━━━━\n\n"
            
            return jsonify({"text": txt})

    return jsonify({"text": f"Commands: `@{BOT_NAME.lower()} search <query>`, `@{BOT_NAME.lower()} latest_issues`"})

# ---------- Other Endpoints (unchanged) ----------
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
    
    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful!"

@app.route('/bot/events', methods=['POST'])
def bot_events():
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    
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
            resp = requests.post(SIGNALS_EVENT_URL, headers={"Content-Type": "application/json"}, json=event_payload)
            print("Signals response:", resp.status_code)
        except Exception as e:
            print("Error publishing to Signals:", e)
    
    return jsonify({"status": "ok"})

@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    
    events = payload.get("events", [])
    if not events:
        return jsonify({"status": "no_events"}), 200
    
    event_obj = events[0]
    outer_data = event_obj.get("data", {})
    inner_data = outer_data.get("raw", {})
    
    # Extract message data
    message_text = inner_data.get("text", "").strip()
    if not message_text:
        return jsonify({"status": "no_message"}), 200
    
    conversation_id = inner_data.get("chat", {}).get("id", "unknown")
    sender_id = inner_data.get("user", {}).get("id", "unknown")
    message_id = inner_data.get("card", {}).get("id", str(uuid.uuid4()))
    timestamp_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    
    # Index the message
    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)
    
    return jsonify({"status": "indexed"}), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
