from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect
import requests
import os
import json
from datetime import datetime, timedelta
from collections import deque
import threading
import base64
import tempfile
import numpy as np

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
import uuid
from datetime import datetime, timezone
app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # change in prod

# ========= OAuth for Cliq (unchanged, optional) =========
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
CONVERSATIONS_TABLE = "conversations"                # Table name (id 56037000000014013)

# ========= Qdrant / Gemini =========
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = "messages_vec"

# ========= Stratus (File Storage) =========
BUCKET_URL = os.getenv("BUCKET_URL")

# ========= Cliq API =========
CLIQ_API_BASE = "https://cliq.zoho.com/api/v2"

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
genai_client = genai.Client()   # uses GEMINI_API_KEY env


# ---------- Embedding + Qdrant helpers ----------

def normalize_message_id(raw_id: str) -> str:
    # Deterministic UUID5 from the raw string
    return str(uuid.uuid5(uuid.NAMESPACE_URL, raw_id))


def embed_text(text: str) -> list[float]:
    res = genai_client.models.embed_content(
        model="gemini-embedding-001",
        contents=text
    )
    return res.embeddings[0].values


def ensure_qdrant_collection(vector_dim: int):
    """Create collection and payload index if needed."""
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
        pass  # already exists


def classify_message(text: str) -> str:
    """Classify using LLM (QuickML) for better accuracy."""
    if not text or len(text.strip()) < 3:
        return "discussion"
    
    try:
        url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {CATALYST_TOKEN}",
            "CATALYST-ORG": CATALYST_ORG_ID,
        }
        prompt = f"Just give one shot answer if it belongs as incident or discussion or resolution. The Statement is: {text[:500]}"
        data = {
            "prompt": prompt,
            "model": "crm-di-qwen_text_14b-fp8-it",
            "system_prompt": "Be concise and factual. Respond with only one word: 'incident', 'discussion', or 'resolution'.",
            "top_p": 0.9,
            "top_k": 50,
            "best_of": 1,
            "temperature": 0.7,
            "max_tokens": 10,
        }
        resp = requests.post(url, json=data, headers=headers, timeout=10)
        if resp.status_code == 200:
            result = resp.json()
            response_text = result.get("response", "").lower().strip()
            if "incident" in response_text:
                return "incident"
            elif "resolution" in response_text or "resolved" in response_text:
                return "response"
            return "discussion"
    except Exception as e:
        print(f"⚠️ LLM classification failed: {e}, falling back to keyword-based")
    
    # Fallback to keyword-based
    t = text.lower()
    if any(w in t for w in ["error", "down", "timeout", "issue", "bug", "problem", "broken", "failed"]):
        return "incident"
    if any(w in t for w in ["fixed", "resolved", "restarted", "deployed", "solved", "working", "recovered"]):
        return "response"
    return "discussion"


# ---------- Data Store helpers ----------
def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category, incident_id=None, status=None):
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
        "time_stamp": timestamp_ms,   # bigint, not datetime string
        "message_text": message_text,
        "category": category,
    }]
    
    # Add optional fields if provided
    if incident_id:
        body[0]["incident_id"] = incident_id
    if status:
        body[0]["status"] = status

    try:
        resp = requests.post(url, headers=headers, json=body, timeout=10)
        print("DS insert status:", resp.status_code, resp.text)
        if resp.status_code == 201:
            return resp.json()[0].get("ROWID")
    except Exception as e:
        print("DS insert exception:", e)
    return None


# def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
#     """Insert one row into conversations table."""
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         print("⚠️ Catalyst config missing; skipping Data Store insert")
#         return None

#     ts_iso = datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat() + "Z"

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
#     body = [{
#         "conversation_id": conversation_id,
#         "message_id": message_id,
#         "sender_id": sender_id,
#         "time_stamp": ts_iso,
#         "message_text": message_text,
#         "category": category,
#     }]

#     try:
#         resp = requests.post(url, headers=headers, json=body, timeout=10)
#         print("DS insert status:", resp.status_code, resp.text)
#         if resp.status_code == 201:
#             return resp.json()[0].get("ROWID")
#     except Exception as e:
#         print("DS insert exception:", e)
#     return None


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


# ---------- Image handling: Stratus + OCR ----------

def upload_to_stratus(file_content: bytes, object_key: str) -> str:
    """Upload file to Zoho Stratus and return URL."""
    if not BUCKET_URL or not CATALYST_TOKEN:
        print("⚠️ Stratus config missing")
        return None
    
    url = f"{BUCKET_URL}/{object_key}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "compress": "false",
        "cache-control": "max-age=3600",
    }
    
    try:
        resp = requests.put(url, data=file_content, headers=headers, timeout=60)
        if resp.status_code in [200, 201, 204]:
            print(f"✅ Uploaded to Stratus: {url}")
            return url
        else:
            print(f"❌ Stratus upload failed: {resp.status_code} - {resp.text}")
            return None
    except Exception as e:
        print(f"❌ Stratus upload exception: {e}")
        return None


def download_and_upload_image(image_url: str, object_key: str) -> str:
    """Download image from URL and upload to Stratus."""
    try:
        img_resp = requests.get(image_url, timeout=30)
        if img_resp.status_code != 200:
            print(f"❌ Failed to download image: {img_resp.status_code}")
            return None
        file_content = img_resp.content
        return upload_to_stratus(file_content, object_key)
    except Exception as e:
        print(f"❌ Image download/upload exception: {e}")
        return None


def extract_text_from_image(image_url: str) -> str:
    """Extract text from image using Catalyst OCR."""
    if not CATALYST_TOKEN or not CATALYST_PROJECT_ID:
        return ""
    
    # Download image from URL
    try:
        img_resp = requests.get(image_url, timeout=30)
        if img_resp.status_code != 200:
            print(f"❌ Failed to download image: {img_resp.status_code}")
            return ""
        image_content = img_resp.content
    except Exception as e:
        print(f"❌ Image download exception: {e}")
        return ""
    
    # Run OCR
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/ml/ocr"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
    }
    
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp_file:
            tmp_file.write(image_content)
            tmp_file.flush()
            
            with open(tmp_file.name, "rb") as f:
                files = {"image": ("image.png", f, "image/png")}
                data = {"language": "eng"}
                resp = requests.post(url, headers=headers, files=files, data=data, timeout=60)
        
        os.unlink(tmp_file.name)
        
        if resp.status_code == 200:
            result = resp.json()
            # Extract text from OCR response (adjust based on actual API response structure)
            text = result.get("text", "") or result.get("data", {}).get("text", "")
            print(f"✅ OCR extracted {len(text)} characters")
            return text
        else:
            print(f"❌ OCR failed: {resp.status_code} - {resp.text}")
            return ""
    except Exception as e:
        print(f"❌ OCR exception: {e}")
        return ""


# ---------- Incident Tracker ----------

class IncidentTracker:
    """Tracks incidents and their discussions."""
    
    def __init__(self, max_incidents=100):
        self.max_incidents = max_incidents
        self.incidents = deque(maxlen=max_incidents)
        self.lock = threading.Lock()
        self.current_incident = None
    
    def add_incident(self, incident_data):
        """Add a new incident."""
        with self.lock:
            incident = {
                'id': incident_data.get('id'),
                'message_id': incident_data.get('message_id'),
                'text': incident_data.get('text', ''),
                'conversation_id': incident_data.get('conversation_id'),
                'sender_id': incident_data.get('sender_id'),
                'timestamp': incident_data.get('timestamp', datetime.now()),
                'status': 'Open',
                'category': incident_data.get('category', 'incident'),
                'discussion': [],
                'linked_message_ids': [incident_data.get('message_id')],
            }
            self.incidents.append(incident)
            if incident['status'] == 'Open':
                self.current_incident = incident
            print(f"📋 Tracked incident: {incident['id']}")
    
    def add_discussion(self, incident_id, message_text, message_id, sender_id):
        """Add discussion message to an incident."""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    incident['discussion'].append({
                        'text': message_text,
                        'message_id': message_id,
                        'sender_id': sender_id,
                        'timestamp': datetime.now()
                    })
                    if message_id not in incident['linked_message_ids']:
                        incident['linked_message_ids'].append(message_id)
                    print(f"💬 Added discussion to incident {incident_id}")
                    return True
            return False
    
    def link_message_to_incident(self, message_text, message_id, conversation_id):
        """Try to link a message to an existing open incident using semantic similarity."""
        with self.lock:
            open_incidents = [i for i in self.incidents if i['status'] == 'Open']
            if not open_incidents:
                return None
            
            # Try semantic similarity for better linking
            try:
                msg_emb = embed_text(message_text)
                best_match = None
                best_score = 0.0
                
                for incident in open_incidents:
                    # Embed incident text
                    incident_emb = embed_text(incident['text'])
                    # Calculate cosine similarity
                    score = np.dot(msg_emb, incident_emb) / (np.linalg.norm(msg_emb) * np.linalg.norm(incident_emb))
                    
                    # Also check if same conversation (boost score)
                    if incident['conversation_id'] == conversation_id:
                        score += 0.1
                    
                    if score > best_score and score > 0.5:  # Threshold for linking
                        best_score = score
                        best_match = incident
                
                if best_match:
                    self.add_discussion(
                        best_match['id'],
                        message_text,
                        message_id,
                        None
                    )
                    print(f"🔗 Semantically linked message to incident {best_match['id']} (score: {best_score:.2f})")
                    return best_match['id']
            except Exception as e:
                print(f"⚠️ Semantic linking failed: {e}, using simple heuristic")
            
            # Fallback: simple heuristic - same conversation and current incident
            if self.current_incident and self.current_incident['conversation_id'] == conversation_id:
                if self.current_incident['status'] == 'Open':
                    self.add_discussion(
                        self.current_incident['id'],
                        message_text,
                        message_id,
                        None
                    )
                    return self.current_incident['id']
            
            return None
    
    def update_incident_status(self, incident_id, status, resolution_text=None):
        """Update incident status."""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    incident['status'] = status
                    if resolution_text:
                        incident['resolution'] = resolution_text
                    if status == 'Resolved' and self.current_incident and self.current_incident['id'] == incident_id:
                        self.current_incident = None
                    print(f"✅ Updated incident {incident_id} to {status}")
                    return True
            return False
    
    def get_open_incidents(self, limit=10):
        """Get open incidents."""
        with self.lock:
            open_incidents = [i for i in self.incidents if i['status'] == 'Open']
            return list(reversed(open_incidents))[:limit]
    
    def get_incident_by_id(self, incident_id):
        """Get incident by ID."""
        with self.lock:
            for incident in reversed(self.incidents):
                if incident['id'] == incident_id:
                    return incident
            return None


# Global incident tracker
incident_tracker = IncidentTracker()


# ---------- Full indexing pipeline ----------
def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text, attachments=None, incident_id=None):
    """Classify, store in Data Store, then embed & index in Qdrant. Handle images if present."""
    # Handle image attachments
    if attachments:
        for att in attachments:
            if att.get("type") == "image" or att.get("content_type", "").startswith("image/"):
                image_url = att.get("url") or att.get("download_url")
                if image_url:
                    print(f"🖼️ Processing image: {image_url}")
                    # Upload to Stratus
                    object_key = f"cliq_images/{message_id}_{att.get('id', 'img')}.png"
                    stratus_url = download_and_upload_image(image_url, object_key)
                    
                    # Extract text via OCR
                    ocr_text = extract_text_from_image(image_url)
                    if ocr_text:
                        message_text += f"\n[Image OCR: {ocr_text}]"
                        print(f"📝 OCR text: {ocr_text[:100]}...")
    
    # Classify using LLM
    category = classify_message(message_text)
    print(f"📋 Category: {category}")

    # If this is an incident, track it
    if category == "incident" and not incident_id:
        incident_id = str(uuid.uuid4())
        incident_tracker.add_incident({
            'id': incident_id,
            'message_id': message_id,
            'text': message_text,
            'conversation_id': conversation_id,
            'sender_id': sender_id,
            'timestamp': datetime.fromtimestamp(timestamp_ms / 1000.0),
            'category': category,
        })
    elif incident_id:
        # Link to existing incident
        incident_tracker.add_discussion(incident_id, message_text, message_id, sender_id)
    elif category == "response":
        # Try to link resolution to open incident
        linked_id = incident_tracker.link_message_to_incident(message_text, message_id, conversation_id)
        if linked_id:
            incident_tracker.update_incident_status(linked_id, "Resolved", message_text)
            print(f"🔗 Linked resolution to incident {linked_id}")
    elif category == "discussion":
        # Try to link discussion to current incident
        linked_id = incident_tracker.link_message_to_incident(message_text, message_id, conversation_id)
        if linked_id:
            print(f"💬 Linked discussion to incident {linked_id}")

    # Store in Data Store
    status = "Resolved" if category == "response" else "Open" if category == "incident" else None
    row_id = insert_into_datastore(
        conversation_id, message_id, sender_id, timestamp_ms, message_text, category, incident_id, status
    )

    # Embed and index in Qdrant
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
            "message_id": message_id,
            "incident_id": incident_id,
            "status": "Resolved" if category == "response" else "Open" if category == "incident" else None,
        },
    )
    qdrant.upsert(QDRANT_COLLECTION, [point])
    print(f"✅ Indexed in Qdrant: cli_id={message_id}, qdrant_id={qdrant_id}")
# def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
#     """Classify, store in Data Store, then embed & index in Qdrant."""
#     category = classify_message(message_text)
#     print(f"📋 Category: {category}")

#     row_id = insert_into_datastore(
#         conversation_id, message_id, sender_id, timestamp_ms, message_text, category
#     )

#     emb = embed_text(message_text)
#     ensure_qdrant_collection(len(emb))

#     point = PointStruct(
#         id=message_id,
#         vector=emb,
#         payload={
#             "conversation_id": conversation_id,
#             "sender_id": sender_id,
#             "category": category,
#             "row_id": row_id,
#         },
#     )
#     qdrant.upsert(QDRANT_COLLECTION, [point])
#     print(f"✅ Indexed in Qdrant: {message_id}")


# ---------- Semantic search ----------

@app.route("/search", methods=["POST"])
def search():
    data = request.get_json(force=True)
    query = data.get("query", "")
    top_k = int(data.get("top_k", 5))
    category = data.get("category")  # optional

    if not query:
        return jsonify({"error": "query required"}), 400

    q_emb = embed_text(query)

    q_filter = None
    if category:
        q_filter = Filter(
            must=[FieldCondition(key="category", match=MatchValue(value=category))]
        )

    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=top_k,
    )

    # After hits = qdrant.search(...)
    ids = [h.payload.get("message_id") for h in hits if h.payload and h.payload.get("message_id")]
    rows = fetch_by_message_ids(ids)

    results = []
    for h in hits:
        orig_id = h.payload.get("message_id") if h.payload else None
        if not orig_id:
            continue
        row = next((r for r in rows if r.get("message_id") == orig_id), None)
        if not row:
            continue
        results.append({
            "message_id": orig_id,
            "score": h.score,
            "message_text": row.get("message_text"),
            "category": row.get("category"),
            "conversation_id": row.get("conversation_id"),
            "sender_id": row.get("sender_id"),
            "time_stamp": row.get("time_stamp"),
        })


    # ids = [str(h.id) for h in hits]
    # rows = fetch_by_message_ids(ids)

    # results = []
    # for h in hits:
    #     row = next((r for r in rows if r.get("message_id") == str(h.id)), None)
    #     if not row:
    #         continue
    #     results.append({
    #         "message_id": h.id,
    #         "score": h.score,
    #         "message_text": row.get("message_text"),
    #         "category": row.get("category"),
    #         "conversation_id": row.get("conversation_id"),
    #         "sender_id": row.get("sender_id"),
    #         "time_stamp": row.get("time_stamp"),
    #     })

    return jsonify({"query": query, "results": results})


@app.route("/incidents", methods=["GET"])
def get_incidents():
    """API endpoint to get open incidents."""
    status = request.args.get("status", "Open")
    limit = int(request.args.get("limit", 10))
    
    if status == "Open":
        incidents = incident_tracker.get_open_incidents(limit=limit)
    else:
        # Get all incidents and filter
        with incident_tracker.lock:
            all_incidents = list(incident_tracker.incidents)
            incidents = [i for i in all_incidents if i['status'] == status][:limit]
    
    return jsonify({
        "status": status,
        "count": len(incidents),
        "incidents": incidents
    })


@app.route("/incidents/<incident_id>", methods=["GET"])
def get_incident(incident_id):
    """API endpoint to get specific incident details."""
    incident = incident_tracker.get_incident_by_id(incident_id)
    if not incident:
        return jsonify({"error": "Incident not found"}), 404
    return jsonify(incident)


# ========= Message Handler: @Workspace-vita commands =========

BOT_NAME = "Workspace-vita"

@app.route('/bot/command', methods=['POST'])
def bot_command():
    """Handle @Workspace-vita mentions from Cliq Message Handler."""
    form_data = request.form.to_dict()
    print("---- Command from Cliq Message Handler ----")
    print(form_data)
    
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    command_text = form_data.get('commandText', '').strip()
    
    if not command_text:
        return jsonify({
            "text": f"🤖 *{BOT_NAME} Assistant*\n\nAvailable commands:\n• `@{BOT_NAME} search <query>` - Search past incidents\n• `@{BOT_NAME} latest_issues` - Show current open issues\n• `@{BOT_NAME} issue <id>` - Get issue details"
        })
    
    try:
        user = json.loads(user_raw) if user_raw else {}
        chat = json.loads(chat_raw) if chat_raw else {}
    except Exception as e:
        print(f"Error parsing user/chat in command: {e}")
        return jsonify({"text": "Sorry, I couldn't read that command."})
    
    # Parse command: "search db timeout" or "latest_issues"
    tokens = command_text.split()
    if not tokens:
        return jsonify({
            "text": f"Usage: `@{BOT_NAME} search <query>` or `@{BOT_NAME} latest_issues`"
        })
    
    cmd = tokens[0].lower()
    args = " ".join(tokens[1:]).strip()
    
    if cmd == "search":
        if not args:
            return jsonify({
                "text": f"Usage: `@{BOT_NAME} search <query>`\n\nExample: `@{BOT_NAME} search database timeout`"
            })
        return handle_search_command(args, chat, user)
    
    elif cmd == "latest_issues" or cmd == "issues":
        return handle_latest_issues(chat, user)
    
    elif cmd == "issue":
        if not args:
            return jsonify({
                "text": f"Usage: `@{BOT_NAME} issue <id>`"
            })
        return handle_issue_details(args, chat, user)
    
    else:
        return jsonify({
            "text": f"❌ Unknown command: `{cmd}`\n\nAvailable commands:\n• `@{BOT_NAME} search <query>`\n• `@{BOT_NAME} latest_issues`\n• `@{BOT_NAME} issue <id>`"
        })


def handle_search_command(query: str, chat: dict, user: dict):
    """Handle @Workspace-vita search <query> command."""
    print(f"🔍 Search command: {query}")
    
    # Use existing semantic search logic
    q_emb = embed_text(query)
    q_filter = Filter(
        must=[FieldCondition(key="category", match=MatchValue(value="incident"))]
    )
    
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=5,
    )
    
    if not hits:
        return jsonify({
            "text": f"🔍 No similar incidents found for: `{query}`\n\nTry a different search term or check if any incidents have been recorded."
        })
    
    ids = [h.payload.get("message_id") for h in hits if h.payload and h.payload.get("message_id")]
    rows = fetch_by_message_ids(ids)
    
    if not rows:
        return jsonify({
            "text": f"🔍 No incidents found for: `{query}`"
        })
    
    lines = [f"🔍 *Past incidents similar to:* `{query}`\n"]
    for i, h in enumerate(hits[:5], 1):
        orig_id = h.payload.get("message_id") if h.payload else None
        if not orig_id:
            continue
        row = next((r for r in rows if r.get("message_id") == orig_id), None)
        if not row:
            continue
        snippet = row.get("message_text", "")[:150]
        score = h.score
        category = row.get("category", "incident")
        timestamp = row.get("time_stamp", "")
        
        # Format timestamp if it's a number
        if isinstance(timestamp, (int, float)):
            try:
                ts_dt = datetime.fromtimestamp(timestamp / 1000.0)
                ts_str = ts_dt.strftime("%Y-%m-%d %H:%M")
            except:
                ts_str = str(timestamp)
        else:
            ts_str = str(timestamp)
        
        lines.append(f"*{i}.* ({score:.2f}) {snippet}...")
        lines.append(f"   📅 {ts_str} | Category: {category}\n")
    
    return jsonify({"text": "\n".join(lines)})


def handle_latest_issues(chat: dict, user: dict):
    """Handle @Workspace-vita latest_issues command."""
    print("📋 Latest issues command")
    
    # First try in-memory tracker
    open_incidents = incident_tracker.get_open_incidents(limit=10)
    
    # Also fetch from Data Store for persistence
    rows = fetch_recent_incidents(limit=10)
    
    # Combine and dedupe
    all_incidents = {}
    
    # Add from tracker
    for incident in open_incidents:
        all_incidents[incident['id']] = {
            'text': incident['text'],
            'id': incident['id'],
            'timestamp': incident['timestamp'],
            'status': incident['status'],
            'discussion_count': len(incident['discussion'])
        }
    
    # Add from Data Store (if not already in tracker)
    for row in rows:
        incident_id = row.get("incident_id")
        if not incident_id:
            # Create a synthetic ID from message_id for display
            incident_id = f"msg_{row.get('message_id', 'unknown')}"
        
        if incident_id not in all_incidents:
            timestamp = row.get("time_stamp")
            if isinstance(timestamp, (int, float)):
                try:
                    ts_dt = datetime.fromtimestamp(timestamp / 1000.0)
                except:
                    ts_dt = datetime.now()
            else:
                ts_dt = datetime.now()
            
            all_incidents[incident_id] = {
                'text': row.get("message_text", ""),
                'id': incident_id,
                'timestamp': ts_dt,
                'status': row.get("status", "Open"),
                'discussion_count': 0
            }
    
    if not all_incidents:
        return jsonify({
            "text": "✅ No open incidents currently."
        })
    
    # Sort by timestamp (most recent first)
    incidents_list = sorted(all_incidents.values(), key=lambda x: x['timestamp'], reverse=True)[:10]
    
    lines = [f"📋 *Latest Open Issues ({len(incidents_list)})*\n"]
    for i, incident in enumerate(incidents_list, 1):
        text_snippet = incident['text'][:120]
        ts_str = incident['timestamp'].strftime("%Y-%m-%d %H:%M") if isinstance(incident['timestamp'], datetime) else str(incident['timestamp'])
        lines.append(f"*{i}.* {text_snippet}...")
        lines.append(f"   🆔 `{incident['id']}` | 💬 {incident['discussion_count']} messages | 📅 {ts_str}\n")
    
    return jsonify({"text": "\n".join(lines)})


def handle_issue_details(issue_id: str, chat: dict, user: dict):
    """Handle @Workspace-vita issue <id> command."""
    print(f"📋 Issue details command: {issue_id}")
    
    # Try to get from tracker first
    incident = incident_tracker.get_incident_by_id(issue_id)
    
    if not incident:
        # Try to fetch from Data Store
        rows = fetch_by_incident_id(issue_id)
        if rows:
            # Reconstruct incident from Data Store
            main_row = rows[0]
            incident = {
                'id': issue_id,
                'text': main_row.get("message_text", ""),
                'status': main_row.get("status", "Unknown"),
                'timestamp': datetime.fromtimestamp(main_row.get("time_stamp", 0) / 1000.0) if isinstance(main_row.get("time_stamp"), (int, float)) else datetime.now(),
                'discussion': []
            }
        else:
            return jsonify({
                "text": f"❌ Issue `{issue_id}` not found."
            })
    
    issue_text = f"📋 *Issue Details: {issue_id}*\n\n"
    issue_text += f"*Status:* {incident.get('status', 'Unknown')}\n"
    issue_text += f"*Category:* {incident.get('category', 'incident')}\n"
    
    ts = incident.get('timestamp')
    if isinstance(ts, datetime):
        ts_str = ts.strftime('%Y-%m-%d %H:%M:%S')
    else:
        ts_str = str(ts)
    issue_text += f"*Time:* {ts_str}\n\n"
    
    issue_text += f"*Initial Message:*\n{incident.get('text', 'N/A')}\n\n"
    
    if incident.get('discussion'):
        issue_text += f"*Discussion ({len(incident['discussion'])} messages):*\n"
        for disc in incident['discussion']:
            issue_text += f"• {disc.get('text', '')[:150]}...\n"
    
    if incident.get('resolution'):
        issue_text += f"\n*Resolution:*\n{incident['resolution']}\n"
    
    return jsonify({"text": issue_text})


def fetch_recent_incidents(limit=10):
    """Fetch recent incidents from Data Store."""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    try:
        resp = requests.get(f"{url}?max_rows={limit * 2}", headers=headers, timeout=10)
        if resp.status_code != 200:
            print("DS fetch error:", resp.status_code, resp.text)
            return []
        rows = resp.json().get("data", [])
        
        # Filter for incidents and open status
        incidents = [r for r in rows if r.get("category") == "incident" and r.get("status") == "Open"]
        
        # Sort by timestamp (most recent first)
        incidents.sort(key=lambda x: x.get("time_stamp", 0), reverse=True)
        
        return incidents[:limit]
    except Exception as e:
        print("DS fetch exception:", e)
        return []


def fetch_by_incident_id(incident_id: str):
    """Fetch messages by incident_id from Data Store."""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    try:
        resp = requests.get(f"{url}?max_rows=200", headers=headers, timeout=10)
        if resp.status_code != 200:
            return []
        rows = resp.json().get("data", [])
        return [r for r in rows if r.get("incident_id") == incident_id]
    except Exception as e:
        print("DS fetch by incident_id exception:", e)
        return []


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
        message_content = raw["message"]["content"]
        message_text = message_content.get("text", "")
        message_id = raw["message"]["id"]
        timestamp_ms = raw["time"]
        attachments = raw["message"].get("attachments", [])
    except KeyError as e:
        print("Missing field in raw:", e)
        return jsonify({"status": "bad_raw"}), 200

    sender_id = user.get("zoho_user_id") or user.get("id")
    conversation_id = chat.get("id")  # or channel_unique_name / channel_id

    print(f"📨 {message_text!r}")
    print(f"   conv={conversation_id}, sender={sender_id}, ts_ms={timestamp_ms}")
    if attachments:
        print(f"   📎 {len(attachments)} attachment(s)")

    # Process message with attachments
    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text, attachments)

    return jsonify({"status": "processed"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)

# from dotenv import load_dotenv
# load_dotenv()

# from flask import Flask, request, jsonify, redirect
# import requests
# import os
# import json
# from datetime import datetime
# from qdrant_client import QdrantClient
# from qdrant_client.models import VectorParams, Distance, PointStruct, PayloadSchemaType, Filter, FieldCondition, MatchValue
# from google import genai

# app = Flask(__name__)
# app.secret_key = 'YOUR_SECRET_KEY'

# # ===== Zoho OAuth =====
# CLIENT_ID = os.getenv('CLIENT_ID')
# CLIENT_SECRET = os.getenv('CLIENT_SECRET')
# REDIRECT_URI = 'http://localhost:8000/oauth/callback'
# AUTH_URL = 'https://accounts.zoho.in/oauth/v2/auth'
# TOKEN_URL = 'https://accounts.zoho.in/oauth/v2/token'
# SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

# ACCESS_TOKEN = None
# REFRESH_TOKEN = None

# # ===== Signals =====
# SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')

# # ===== Catalyst Data Store =====
# CATALYST_TOKEN = os.getenv("CATALYST_TOKEN")
# CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
# CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
# CONVERSATIONS_TABLE_ID = "56037000000014013"  # Your table ID
# CONVERSATIONS_TABLE = "conversations"

# # ===== Qdrant =====
# QDRANT_URL = os.getenv("QDRANT_URL")
# QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
# QDRANT_COLLECTION = "messages_vec"

# # ===== Clients =====
# qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
# genai_client = genai.Client()


# # ========== EMBEDDING & QDRANT FUNCTIONS ==========

# def embed_text(text: str) -> list[float]:
#     """Generate embedding using Gemini"""
#     res = genai_client.models.embed_content(
#         model="gemini-embedding-001",
#         contents=text
#     )
#     return res.embeddings[0].values


# def ensure_qdrant_collection(vector_dim: int):
#     """Ensure Qdrant collection exists with payload index"""
#     collections = qdrant.get_collections().collections
#     if not any(c.name == QDRANT_COLLECTION for c in collections):
#         qdrant.create_collection(
#             collection_name=QDRANT_COLLECTION,
#             vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE)
#         )
#         print(f"✅ Created Qdrant collection: {QDRANT_COLLECTION}")
    
#     try:
#         qdrant.create_payload_index(
#             collection_name=QDRANT_COLLECTION,
#             field_name="category",
#             field_schema=PayloadSchemaType.KEYWORD
#         )
#     except Exception as e:
#         pass  # Index might already exist


# def classify_message(text: str) -> str:
#     """Simple keyword-based classification (replace with QuickML later)"""
#     text_lower = text.lower()
#     if any(word in text_lower for word in ['error', 'down', 'timeout', 'issue', 'problem', 'bug']):
#         return "incident"
#     elif any(word in text_lower for word in ['fixed', 'resolved', 'restarted', 'deployed', 'solved']):
#         return "response"
#     else:
#         return "discussion"


# # ========== DATA STORE FUNCTIONS ==========

# def insert_into_datastore(conversation_id, message_id, sender_id, timestamp_ms, message_text, category):
#     """Insert message row into Catalyst Data Store"""
#     if not CATALYST_TOKEN or not CATALYST_PROJECT_ID:
#         print("⚠️ Catalyst config missing; skipping Data Store insert")
#         return None
    
#     ts_iso = datetime.utcfromtimestamp(timestamp_ms / 1000.0).isoformat() + "Z"
    
#     url = f"https://api.catalyst.zoho.in/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     body = [{
#         "conversation_id": conversation_id,
#         "message_id": message_id,
#         "sender_id": sender_id,
#         "time_stamp": ts_iso,
#         "message_text": message_text,
#         "category": category,
#     }]
    
#     try:
#         resp = requests.post(url, headers=headers, json=body, timeout=10)
#         print(f"✅ Data Store insert: {resp.status_code}")
#         if resp.status_code == 201:
#             result = resp.json()
#             return result[0].get("ROWID")  # Return ROWID for reference
#         else:
#             print(f"❌ Data Store error: {resp.text}")
#             return None
#     except Exception as e:
#         print(f"❌ Data Store exception: {e}")
#         return None


# def fetch_messages_by_ids(message_ids: list[str]):
#     """Fetch full message details from Data Store by message_ids"""
#     if not CATALYST_TOKEN or not CATALYST_PROJECT_ID or not message_ids:
#         return []
    
#     url = f"https://api.catalyst.zoho.in/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     # Fetch all rows (you can optimize with filters if Data Store supports it)
#     # For now, we fetch and filter in memory
#     try:
#         resp = requests.get(f"{url}?max_rows=100", headers=headers, timeout=10)
#         if resp.status_code == 200:
#             data = resp.json()
#             rows = data.get("data", [])
#             # Filter by message_ids
#             return [r for r in rows if r.get("message_id") in message_ids]
#         return []
#     except Exception as e:
#         print(f"❌ Fetch error: {e}")
#         return []


# # ========== INDEXING PIPELINE ==========

# def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
#     """Full pipeline: classify → embed → store in Data Store + Qdrant"""
    
#     # 1) Classify
#     category = classify_message(message_text)
#     print(f"📋 Classified as: {category}")
    
#     # 2) Store in Data Store
#     row_id = insert_into_datastore(
#         conversation_id, message_id, sender_id, timestamp_ms, message_text, category
#     )
    
#     # 3) Generate embedding
#     emb = embed_text(message_text)
#     dim = len(emb)
#     ensure_qdrant_collection(dim)
    
#     # 4) Index in Qdrant
#     point = PointStruct(
#         id=message_id,
#         vector=emb,
#         payload={
#             "conversation_id": conversation_id,
#             "sender_id": sender_id,
#             "category": category,
#             "row_id": row_id,  # Store Data Store ROWID for reference
#         },
#     )
#     qdrant.upsert(QDRANT_COLLECTION, [point])
#     print(f"✅ Indexed in Qdrant: {message_id}")


# # ========== SEARCH API ==========

# @app.route("/search", methods=["POST"])
# def search():
#     data = request.get_json(force=True)
#     query = data.get("query", "")
#     top_k = int(data.get("top_k", 5))
#     category = data.get("category")

#     if not query:
#         return jsonify({"error": "query required"}), 400

#     q_emb = embed_text(query)

#     q_filter = None
#     if category:
#         q_filter = Filter(
#             must=[FieldCondition(key="category", match=MatchValue(value=category))]
#         )

#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=q_emb,
#         query_filter=q_filter,
#         limit=top_k,
#     )

#     ids = [str(h.id) for h in hits]
#     rows = fetch_by_message_ids(ids)

#     results = []
#     for h in hits:
#         row = next((r for r in rows if r.get("message_id") == str(h.id)), None)
#         if not row:
#             continue
#         results.append({
#             "message_id": h.id,
#             "score": h.score,
#             "message_text": row.get("message_text"),
#             "category": row.get("category"),
#             "conversation_id": row.get("conversation_id"),
#             "sender_id": row.get("sender_id"),
#             "time_stamp": row.get("time_stamp"),
#         })

#     return jsonify({"query": query, "results": results})
# # ========== OAUTH (unchanged) ==========

# @app.route('/login')
# def login():
#     params = {
#         'client_id': CLIENT_ID,
#         'scope': SCOPE,
#         'response_type': 'code',
#         'redirect_uri': REDIRECT_URI,
#         'access_type': 'offline',
#         'prompt': 'consent'
#     }
#     auth_request = requests.Request('GET', AUTH_URL, params=params).prepare()
#     return redirect(auth_request.url)


# @app.route('/oauth/callback')
# def oauth_callback():
#     global ACCESS_TOKEN, REFRESH_TOKEN
#     code = request.args.get('code')
#     if not code:
#         return "Missing authorization code", 400
    
#     data = {
#         'code': code,
#         'client_id': CLIENT_ID,
#         'client_secret': CLIENT_SECRET,
#         'redirect_uri': REDIRECT_URI,
#         'grant_type': 'authorization_code'
#     }
#     resp = requests.post(TOKEN_URL, data=data)
#     token_json = resp.json()
    
#     if 'access_token' not in token_json:
#         error = token_json.get('error', 'Unknown error')
#         error_description = token_json.get('error_description', '')
#         return f"Failed to get access token: {error} - {error_description}", 400
    
#     ACCESS_TOKEN = token_json.get('access_token')
#     REFRESH_TOKEN = token_json.get('refresh_token')
#     return "OAuth Successful!"


# # ========== PRODUCER: Deluge → Flask → Signals ==========

# @app.route('/bot/events', methods=['POST'])
# def bot_events():
#     form_data = request.form.to_dict()
#     print("---- Received from Deluge ----")
#     print(form_data)
    
#     operation = form_data.get('operation')
#     user_raw = form_data.get('user')
#     chat_raw = form_data.get('chat')
#     data_raw = form_data.get('data')
    
#     inner_data = {
#         "source": "zoho_cliq_bot",
#         "operation": operation,
#         "user": user_raw,
#         "chat": chat_raw,
#         "raw": data_raw
#     }
    
#     event_payload = {"data": inner_data}
    
#     if SIGNALS_EVENT_URL:
#         try:
#             resp = requests.post(
#                 SIGNALS_EVENT_URL,
#                 headers={"Content-Type": "application/json"},
#                 json=event_payload
#             )
#             print("Signals response:", resp.status_code, resp.text)
#         except Exception as e:
#             print("Error publishing to Signals:", e)
    
#     return jsonify({"status": "ok"})


# # ========== CONSUMER: Signals → Flask → Data Store + Qdrant ==========

# @app.route('/signals/consume', methods=['POST'])
# def signals_consume():
#     payload = request.get_json()
#     print("==== Signals delivered queued event ====")
#     print(json.dumps(payload, indent=2))

#     events = payload.get("events", [])
#     if not events:
#         return jsonify({"status": "no_events"}), 200

#     event_obj = events[0]
#     outer_data = event_obj.get("data", {})
#     inner_data = outer_data.get("data", {})

#     raw_str = inner_data.get("raw")
#     user_str = inner_data.get("user")
#     chat_str = inner_data.get("chat")

#     if not (raw_str and user_str and chat_str):
#         print("Missing raw/user/chat in inner_data; nothing to index")
#         return jsonify({"status": "ignored"}), 200

#     try:
#         raw = json.loads(raw_str)
#         user = json.loads(user_str)
#         chat = json.loads(chat_str)
#     except Exception as e:
#         print("Error parsing JSON fields:", e)
#         return jsonify({"status": "parse_error"}), 200

#     # Cliq structure: confirm with one log
#     print("Parsed raw:", json.dumps(raw, indent=2))
#     print("Parsed user:", json.dumps(user, indent=2))
#     print("Parsed chat:", json.dumps(chat, indent=2))

#     try:
#         message_text = raw["message"]["content"]["text"]
#         message_id = raw["message"]["id"]
#         timestamp_ms = raw["time"]
#     except KeyError as e:
#         print("Missing field in raw:", e)
#         return jsonify({"status": "bad_raw"}), 200

#     sender_id = user.get("zoho_user_id") or user.get("id")
#     conversation_id = chat.get("id")  # or channel_unique_name / channel_id

#     print(f"📨 {message_text!r}")
#     print(f"   conv={conversation_id}, sender={sender_id}, ts_ms={timestamp_ms}")

#     # Now run full indexing pipeline
#     index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)

#     return jsonify({"status": "processed"})
    



# if __name__ == '__main__':
#     app.run(port=8000, debug=True)
