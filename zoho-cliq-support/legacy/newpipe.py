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

# In-memory issue tracker (for fast access)
open_issues = {}  # {issue_id: {"opened_at": ts, "title": str, "category": str, "severity": str}}

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
  - "resolution" ONLY if the message explicitly states that a problem is ALREADY fixed, resolved, closed, working now, back to normal, or issue is gone. Examples: "fixed it", "issue resolved", "back to normal now", "working fine now"
  - "incident" if it reports an active problem, failure, error, outage, or something broken
  - "discussion" for everything else including: suggestions ("let's try"), questions ("should we?"), plans ("we need to"), explanations, acknowledgments, or any chat that is NOT an active incident report or a completion statement
- category: database, cache, auth, network, security, deployment, or other
- severity: low, medium, or high

JSON:"""

    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "You classify engineering messages. Return ONLY valid JSON. Be strict: 'resolution' means the problem is ALREADY solved, not planned or being worked on.",
        "top_p": 0.8,
        "top_k": 40,
        "best_of": 1,
        "temperature": 0.1,
        "max_tokens": 128,
    }

    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        print(f"LLM Status: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"LLM Error Response: {resp.text}")
            return {"role": "discussion", "category": "other", "severity": "low"}
        
        result = resp.json()
        print(f"LLM Full Response: {json.dumps(result, indent=2)}")
        
        # Try different paths to find the output
        output_text = None
        
        # Path 1: data.output_text
        if "data" in result and "output_text" in result["data"]:
            output_text = result["data"]["output_text"]
        
        # Path 2: direct output_text
        elif "output_text" in result:
            output_text = result["output_text"]
        
        # Path 3: response
        elif "response" in result:
            output_text = result["response"]
        
        print(f"Extracted output_text: {output_text}")
        
        if not output_text:
            print("No output_text found in response")
            return {"role": "discussion", "category": "other", "severity": "low"}
        
        # Clean the output (remove markdown, extra text)
        output_text = output_text.strip()
        
        # Try to extract JSON if surrounded by text
        if "{" in output_text and "}" in output_text:
            start = output_text.index("{")
            end = output_text.rindex("}") + 1
            json_str = output_text[start:end]
        else:
            json_str = output_text
        
        print(f"Attempting to parse JSON: {json_str}")
        classification = json.loads(json_str)
        
        # Validate fields
        if "role" not in classification or "category" not in classification or "severity" not in classification:
            print(f"Missing required fields in classification: {classification}")
            return {"role": "discussion", "category": "other", "severity": "low"}
        
        print(f"✅ Parsed classification: {classification}")
        return classification
        
    except json.JSONDecodeError as e:
        print(f"JSON parse error: {e}")
        print(f"Failed to parse: {output_text if 'output_text' in locals() else 'N/A'}")
        return {"role": "discussion", "category": "other", "severity": "low"}
    except Exception as e:
        print(f"LLM classification exception: {e}")
        import traceback
        traceback.print_exc()
        return {"role": "discussion", "category": "other", "severity": "low"}



def get_latest_open_issue_for_conversation(conversation_id: str):
    """
    Return the most recently opened issue for this conversation_id that is still Open.
    """
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return None

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        resp = requests.get(f"{url}?max_rows=300", headers=headers, timeout=10)
        if resp.status_code != 200:
            print("DS fetch issues (by conv) error:", resp.status_code, resp.text)
            return None
        rows = resp.json().get("data", [])
        # Filter by source=Cliq and status Open, and optionally by conversation_id if you store it in issues
        open_rows = [r for r in rows if isinstance(r.get("status"), str) and r["status"].strip().lower() == "open"]
        # If you have a conversation_id column in issues, filter by that too.
        # open_rows = [r for r in open_rows if r.get("conversation_id") == conversation_id]
        if not open_rows:
            return None
        open_rows.sort(key=lambda r: int(r.get("opened_at", 0)), reverse=True)
        return open_rows[0]
    except Exception as e:
        print("get_latest_open_issue_for_conversation exception:", e)
        return None



# def classify_message_llm(text: str) -> dict:
#     """
#     Classify message using QuickML LLM.
#     Returns: {"role": "...", "category": "...", "severity": "..."}
#     """
#     url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     prompt = f"""Classify this engineering message:

# Statement: "{text}"

# Respond with ONLY this JSON format (no extra text):
# {{"role": "incident", "category": "database", "severity": "high"}}

# Rules:
# - role: "incident" if reporting a problem/error, "resolution" if fixing/closing issue, "discussion" otherwise
# - category: database, cache, auth, network, security, deployment, or other
# - severity: low, medium, or high

# JSON:"""

#     data = {
#         "prompt": prompt,
#         "model": "crm-di-qwen_text_14b-fp8-it",
#         "system_prompt": "You classify engineering messages. Return ONLY valid JSON.",
#         "top_p": 0.9,
#         "top_k": 50,
#         "best_of": 1,
#         "temperature": 0.3,
#         "max_tokens": 128,
#     }
    
#     try:
#         resp = requests.post(url, json=data, headers=headers, timeout=30)
#         print(f"LLM Status: {resp.status_code}")
        
#         if resp.status_code != 200:
#             print(f"LLM Error Response: {resp.text}")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         result = resp.json()
#         print(f"LLM Full Response: {json.dumps(result, indent=2)}")
        
#         # Try different paths to find the output
#         output_text = None
        
#         # Path 1: data.output_text
#         if "data" in result and "output_text" in result["data"]:
#             output_text = result["data"]["output_text"]
        
#         # Path 2: direct output_text
#         elif "output_text" in result:
#             output_text = result["output_text"]
        
#         # Path 3: response
#         elif "response" in result:
#             output_text = result["response"]
        
#         print(f"Extracted output_text: {output_text}")
        
#         if not output_text:
#             print("No output_text found in response")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         # Clean the output (remove markdown, extra text)
#         output_text = output_text.strip()
        
#         # Try to extract JSON if surrounded by text
#         if "{" in output_text and "}" in output_text:
#             start = output_text.index("{")
#             end = output_text.rindex("}") + 1
#             json_str = output_text[start:end]
#         else:
#             json_str = output_text
        
#         print(f"Attempting to parse JSON: {json_str}")
#         classification = json.loads(json_str)
        
#         # Validate fields
#         if "role" not in classification or "category" not in classification or "severity" not in classification:
#             print(f"Missing required fields in classification: {classification}")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         print(f"✅ Parsed classification: {classification}")
#         return classification
        
#     except json.JSONDecodeError as e:
#         print(f"JSON parse error: {e}")
#         print(f"Failed to parse: {output_text if 'output_text' in locals() else 'N/A'}")
#         return {"role": "discussion", "category": "other", "severity": "low"}
#     except Exception as e:
#         print(f"LLM classification exception: {e}")
#         import traceback
#         traceback.print_exc()
#         return {"role": "discussion", "category": "other", "severity": "low"}


# ---------- Embedding + Qdrant ----------

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
        print("⚠️ Catalyst config missing; skipping DS insert")
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
        print("DS message insert:", resp.status_code)
        if resp.status_code == 201:
            return resp.json()[0].get("ROWID")
    except Exception as e:
        print("DS message insert exception:", e)
    return None

def fetch_messages_by_issue_id(issue_id: str):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        # ✅ PAGINATE to get ALL messages (not just first 500)
        all_rows = []
        next_token = None
        
        while True:
            params = f"max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"
            
            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            print(f"DS fetch messages (issue={issue_id}): {resp.status_code}")
            
            if resp.status_code != 200:
                print(f"❌ Fetch failed: {resp.text}")
                break
            
            body = resp.json()
            rows = body.get("data", [])
            matching_rows = [r for r in rows if r.get("issue_id") == issue_id]
            all_rows.extend(matching_rows)
            
            next_token = body.get("next_token")
            if not next_token:
                break
        
        print(f"✅ Found {len(all_rows)} messages for issue {issue_id}")
        return all_rows
        
    except Exception as e:
        print(f"fetch_messages_by_issue_id exception: {e}")
        return []

# def fetch_messages_by_issue_id(issue_id: str):
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         return []

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     try:
#         resp = requests.get(f"{url}?max_rows=500", headers=headers, timeout=10)
#         if resp.status_code != 200:
#             return []
#         rows = resp.json().get("data", [])
#         return [r for r in rows if r.get("issue_id") == issue_id]
#     except Exception:
#         return []


# ---------- Data Store: Issues ----------

def create_issue_in_datastore(issue_id, title, source, category, severity, opened_at):
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return None

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
        print("DS issue create:", resp.status_code)
        return resp.status_code == 201
    except Exception as e:
        print("DS issue create exception:", e)
        return False

def close_issue_in_datastore(issue_id: str, resolved_at_ms: int) -> bool:
    """
    Mark issue as Resolved in the issues table.
    Uses ROWID + PUT as in Catalyst docs.
    """
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        print("⚠️ Missing Catalyst token / project id")
        return False

    base_url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "Content-Type": "application/json",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        # 1) Fetch ALL issues to find this specific issue_id
        get_resp = requests.get(f"{base_url}?max_rows=300", headers=headers, timeout=10)
        print(f"DS fetch for close (issue_id={issue_id}): {get_resp.status_code}")
        
        if get_resp.status_code != 200:
            print(f"❌ Failed to fetch issues: {get_resp.text}")
            return False

        rows = get_resp.json().get("data", [])
        print(f"Found {len(rows)} total issue rows")
        
        # Find the row with matching issue_id
        issue_row = None
        for r in rows:
            if r.get("issue_id") == issue_id:
                issue_row = r
                break
        
        if not issue_row:
            print(f"❌ No issue row found for issue_id={issue_id}")
            print(f"Available issue_ids: {[r.get('issue_id') for r in rows[:5]]}")
            return False

        row_id = issue_row.get("ROWID")
        current_status = issue_row.get("status")
        
        print(f"Found issue row: ROWID={row_id}, current_status={current_status}")
        
        if not row_id:
            print(f"❌ No ROWID for issue_id={issue_id}")
            return False

        # 2) Update ONLY the status and resolved_at fields
        update_body = [{
            "ROWID": row_id,
            "status": "Resolved",
            "resolved_at": str(int(resolved_at_ms)),
        }]

        print(f"Updating issue with body: {update_body}")
        put_resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
        print(f"DS issue close PUT: {put_resp.status_code} {put_resp.text}")

        if put_resp.status_code == 200:
            print(f"✅ Successfully updated issue {issue_id} to Resolved")
            return True
        else:
            print(f"❌ PUT failed with status {put_resp.status_code}")
            return False

    except Exception as e:
        print(f"❌ DS issue close exception: {e}")
        import traceback
        traceback.print_exc()
        return False


# def close_issue_in_datastore(issue_id: str, resolved_at_ms: int) -> bool:
#     """
#     Mark issue as Resolved in the issues table.
#     Uses ROWID + PUT as in Catalyst docs.
#     """
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         print("⚠️ Missing Catalyst token / project id")
#         return False

#     base_url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     try:
#         # 1) Fetch issues to get ROWID for this issue_id
#         get_resp = requests.get(f"{base_url}?max_rows=300", headers=headers, timeout=10)
#         print("DS fetch issues (for close):", get_resp.status_code, get_resp.text)
#         if get_resp.status_code != 200:
#             return False

#         rows = get_resp.json().get("data", [])
#         issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
#         if not issue_row:
#             print(f"❌ No issue row found for issue_id={issue_id}")
#             return False

#         row_id = issue_row.get("ROWID")
#         if not row_id:
#             print(f"❌ No ROWID for issue_id={issue_id}")
#             return False

#         # 2) Build update payload exactly like docs
#         update_body = [{
#             "ROWID": row_id,
#             "status": "Resolved",
#             "resolved_at": str(int(resolved_at_ms)),  # send as string or number, both ok
#         }]

#         put_resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
#         print("DS issue close PUT:", put_resp.status_code, put_resp.text)

#         # Catalyst usually returns 200 with {"status":"success"...}
#         if put_resp.status_code == 200:
#             return True
#         return False

#     except Exception as e:
#         print("DS issue close exception:", e)
#         return False


# def close_issue_in_datastore(issue_id, resolved_at):
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         return False

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     try:
#         # 1) Fetch all issues
#         resp = requests.get(f"{url}?max_rows=300", headers=headers, timeout=10)
#         print("DS fetch issues (for close):", resp.status_code, resp.text)
#         if resp.status_code != 200:
#             return False

#         rows = resp.json().get("data", [])
#         issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
#         if not issue_row:
#             print(f"No issue row found for issue_id={issue_id}")
#             return False

#         row_id = issue_row.get("ROWID")

#         update_body = [{
#             "ROWID": row_id,
#             "status": "Resolved",
#             "resolved_at": str(int(resolved_at)),  # send as string to match DS response style
#         }]

#         resp = requests.put(url, headers=headers, json=update_body, timeout=10)
#         print("DS issue close PUT:", resp.status_code, resp.text)
#         return resp.status_code in (200, 201)
#     except Exception as e:
#         print("DS issue close exception:", e)
#         return False

# def close_issue_in_datastore(issue_id, resolved_at):
#     """Update issue status to Resolved"""
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         return False

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "Content-Type": "application/json",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     # First, fetch the issue to get its ROWID
#     try:
#         resp = requests.get(f"{url}?max_rows=500", headers=headers, timeout=10)
#         if resp.status_code != 200:
#             return False
#         rows = resp.json().get("data", [])
#         issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
#         if not issue_row:
#             return False

#         row_id = issue_row.get("ROWID")
        
#         # Update the row
#         update_body = [{
#             "ROWID": row_id,
#             "status": "Resolved",
#             "resolved_at": int(resolved_at),
#         }]
        
#         resp = requests.put(url, headers=headers, json=update_body, timeout=10)
#         print(f"DS issue close: {resp.status_code}")
#         return resp.status_code in [200, 201]
#     except Exception as e:
#         print(f"DS issue close exception: {e}")
#         return False


# def fetch_open_issues():
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         return []

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     try:
#         resp = requests.get(f"{url}?max_rows=500", headers=headers, timeout=10)
#         if resp.status_code != 200:
#             return []
#         rows = resp.json().get("data", [])
#         open_issues_list = [r for r in rows if r.get("status") == "Open"]
#         open_issues_list.sort(key=lambda r: r.get("opened_at", 0), reverse=True)
#         return open_issues_list
#     except Exception:
#         return []


def fetch_open_issues():
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    all_rows = []
    next_token = None

    try:
        while True:
            params = "max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"

            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            print("DS fetch issues:", resp.status_code, resp.text)
            if resp.status_code != 200:
                break

            body = resp.json()
            rows = body.get("data", [])
            all_rows.extend(rows)

            next_token = body.get("next_token")
            if not next_token:
                break

        open_issues_list = []
        for r in all_rows:
            s = r.get("status")
            if isinstance(s, str) and s.strip().lower() == "open":
                open_issues_list.append(r)

        open_issues_list.sort(key=lambda r: int(r.get("opened_at", 0)), reverse=True)
        return open_issues_list

    except Exception as e:
        print("fetch_open_issues exception:", e)
        return []


# def fetch_open_issues():
#     if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
#         return []

#     url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
#     headers = {
#         "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }

#     all_rows = []
#     next_token = None

#     try:
#         while True:
#             params = "max_rows=300"
#             if next_token:
#                 params += f"&next_token={next_token}"

#             resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
#             print("DS fetch issues:", resp.status_code, resp.text)

#             if resp.status_code != 200:
#                 break

#             body = resp.json()
#             rows = body.get("data", [])
#             all_rows.extend(rows)

#             next_token = body.get("next_token")
#             if not next_token:
#                 break

#         open_issues_list = []
#         for r in all_rows:
#             s = r.get("status")
#             if isinstance(s, str) and s.strip().lower() == "open":
#                 open_issues_list.append(r)

#         open_issues_list.sort(key=lambda r: r.get("opened_at", 0), reverse=True)
#         return open_issues_list

#     except Exception as e:
#         print("fetch_open_issues exception:", e)
#         return []


# ---------- Issue Linking Logic ----------
def find_similar_open_incident(message_emb, threshold=0.75):
    """
    Search for similar open incidents in Qdrant.
    Returns (issue_id, score) if found above threshold, else (None, 0).
    """
    # Get list of open issues from Data Store
    open_rows = fetch_open_issues()
    open_ids = {r.get("issue_id") for r in open_rows if r.get("issue_id")}

    if not open_ids:
        return (None, 0)

    q_filter = Filter(
        must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
    )

    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=message_emb,
        query_filter=q_filter,
        limit=5,
    )

    best_issue_id = None
    best_score = 0.0

    for h in hits:
        if not h.payload:
            continue
        issue_id = h.payload.get("issue_id")
        if not issue_id or issue_id not in open_ids:
            continue
        if h.score > best_score and h.score >= threshold:
            best_issue_id = issue_id
            best_score = h.score

    return (best_issue_id, best_score)

# def find_similar_open_incident(message_emb, threshold=0.75):
#     """
#     Search for similar open incidents in Qdrant.
#     Returns (issue_id, score) if found above threshold, else (None, 0).
#     """
#     q_filter = Filter(
#         must=[
#             FieldCondition(key="role", match=MatchValue(value="incident")),
#         ]
#     )
    
#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=message_emb,
#         query_filter=q_filter,
#         limit=3,
#     )
    
#     for h in hits:
#         if h.score >= threshold and h.payload:
#             issue_id = h.payload.get("issue_id")
#             if issue_id and issue_id in open_issues:
#                 return (issue_id, h.score)
    
#     return (None, 0)


def summarize_resolution_with_llm(messages: list) -> str:
    """Generate resolution summary from linked messages"""
    if not messages:
        return "No resolution details available."
    
    # Get incident + discussion + resolution messages
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

Provide a concise 1-2 sentence summary of WHAT was done to fix it and the outcome."""
    
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Summarize incident resolutions concisely: what was done + outcome.",
        "temperature": 0.3,
        "max_tokens": 150,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        if resp.status_code == 200:
            result = resp.json()
            summary = result.get("data", {}).get("output_text", "").strip()
            return summary if summary else "Resolved (details unavailable)."
    except Exception as e:
        print(f"LLM summarization error: {e}")
    
    return "Issue resolved."


# def summarize_resolution_with_llm(messages: list) -> str:
#     """
#     Use LLM to generate resolution summary from all linked messages.
#     """
#     if not messages:
#         return "No resolution details available."
    
#     # Collect resolution and discussion messages
#     relevant = [m for m in messages if m.get("role") in ["resolution", "discussion"]]
#     if not relevant:
#         return "Issue resolved (no details recorded)."
    
#     text_concat = "\n".join([m.get("message_text", "") for m in relevant[:10]])
    
#     url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     prompt = f"""Summarize how this incident was resolved based on these messages:

# {text_concat}

# Provide a concise 2-3 sentence summary of the resolution."""

#     data = {
#         "prompt": prompt,
#         "model": "crm-di-qwen_text_14b-fp8-it",
#         "system_prompt": "You summarize incident resolutions clearly and concisely.",
#         "temperature": 0.5,
#         "max_tokens": 256,
#     }
    
#     try:
#         resp = requests.post(url, json=data, headers=headers, timeout=30)
#         if resp.status_code == 200:
#             result = resp.json()
#             summary = result.get("data", {}).get("output_text", "").strip()
#             return summary if summary else "Resolution details unavailable."
#     except Exception as e:
#         print(f"LLM summarization error: {e}")
    
#     return "Resolution completed (summary unavailable)."

def store_resolution_summary(issue_id: str, summary: str, resolved_at_ms: int) -> bool:
    """Store resolution summary in issues table"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return False

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

        # ✅ Store in resolution_summary column (add this column in DataStore!)
        update_body = [{
            "ROWID": row_id,
            "status": "Resolved",
            "resolved_at": str(int(resolved_at_ms)),
            "resolution_summary": summary[:500]
        }]

        put_resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
        print(f"✅ DS store resolution: {put_resp.status_code}")
        return put_resp.status_code == 200

    except Exception as e:
        print(f"Store resolution exception: {e}")
        return False


# ---------- Main Indexing Pipeline ----------

def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
    """
    1. Classify with LLM
    2. Embed message
    3. Link to issue (or create new issue)
    4. Store in DS + Qdrant
    """
    # 1. LLM Classification
    cls = classify_message_llm(message_text)
    role = cls.get("role", "discussion")
    category = cls.get("category", "other")
    severity = cls.get("severity", "low")
    
    print(f"📋 Role: {role}, Category: {category}, Severity: {severity}")
    
    # 2. Embed
    emb = embed_text(message_text)
    ensure_qdrant_collection(len(emb))
    
    issue_id = None
    
    # 3. Link to issue
    if role == "incident":
        # 3A-1. Reuse existing open issue with identical title (avoid duplicates)
        existing_open = fetch_open_issues()
        normalized_title = message_text.strip().lower()
        for row in existing_open:
            if row.get("title", "").strip().lower() == normalized_title:
                issue_id = row.get("issue_id")
                print(f"🔗 Reusing existing open issue for same title: {issue_id}")
                break

        # 3A-2. If not found by title, try similarity to other open incidents
        if not issue_id:
            q_filter = Filter(
                must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
            )
            hits = qdrant.search(
                collection_name=QDRANT_COLLECTION,
                query_vector=emb,
                query_filter=q_filter,
                limit=5,
            )
            open_ids = {r.get("issue_id") for r in existing_open if r.get("issue_id")}
            best_issue_id = None
            best_score = 0.0
            for h in hits:
                if not h.payload:
                    continue
                cand_id = h.payload.get("issue_id")
                if not cand_id or cand_id not in open_ids:
                    continue
                if h.score > best_score and h.score >= 0.75:
                    best_issue_id = cand_id
                    best_score = h.score
            if best_issue_id:
                issue_id = best_issue_id
                print(f"🔗 Linked incident to similar open issue: {issue_id} (score={best_score:.2f})")

        # 3A-3. If still no issue_id, create a new issue
        if not issue_id:
            issue_id = str(uuid.uuid4())
            title = message_text[:100] + ("..." if len(message_text) > 100 else "")
            create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
            print(f"🆕 Created new issue: {issue_id}")
    # if role == "incident":
    #     # Check if similar open incident exists
    #     similar_issue_id, score = find_similar_open_incident(emb, threshold=0.75)
        
    #     if similar_issue_id:
    #         print(f"🔗 Linked to existing issue: {similar_issue_id} (score={score:.2f})")
    #         issue_id = similar_issue_id
    #     else:
    #         # Create new issue
    #         issue_id = str(uuid.uuid4())
    #         title = message_text[:100] + ("..." if len(message_text) > 100 else "")
    #         open_issues[issue_id] = {
    #             "opened_at": timestamp_ms,
    #             "title": title,
    #             "category": category,
    #             "severity": severity,
    #         }
    #         create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
    #         print(f"🆕 Created new issue: {issue_id}")
    
    # elif role in ["discussion", "resolution"]:
    #     # Link to nearest open issue
    #     similar_issue_id, score = find_similar_open_incident(emb, threshold=0.65)
    #     if similar_issue_id:
    #         issue_id = similar_issue_id
    #         print(f"🔗 Linked {role} to issue: {issue_id} (score={score:.2f})")
            
    #         # If resolution, check if we should close the issue
    #         if role == "resolution":
    #             # Simple heuristic: if message says "fixed", "resolved", "closed", close it
    #             if any(w in message_text.lower() for w in ["fixed", "resolved", "closed", "done"]):
    #                 close_issue_in_datastore(issue_id, timestamp_ms)
    #                 if issue_id in open_issues:
    #                     del open_issues[issue_id]
    #                 print(f"✅ Closed issue: {issue_id}")
    
    # elif role in ["discussion", "resolution"]:
    # # Link to nearest open issue using Qdrant, but don't depend on in-memory open_issues
    #     q_filter = Filter(
    #         must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
    #     )
    #     hits = qdrant.search(
    #         collection_name=QDRANT_COLLECTION,
    #         query_vector=emb,
    #         query_filter=q_filter,
    #         limit=3,
    #     )
    #     linked_issue_id = None
    #     best_score = 0.0

    #     for h in hits:
    #         if not h.payload:
    #             continue
    #         candidate_issue_id = h.payload.get("issue_id")
    #         if not candidate_issue_id:
    #             continue
    #         # Check if this issue is still open in Data Store
    #         open_issues_rows = fetch_open_issues()
    #         if any(row.get("issue_id") == candidate_issue_id for row in open_issues_rows):
    #             linked_issue_id = candidate_issue_id
    #             best_score = h.score
    #             break

    #     if linked_issue_id:
    #         issue_id = linked_issue_id
    #         print(f"🔗 Linked {role} to issue: {issue_id} (score={best_score:.2f})")

    #         # If resolution, close the issue
    #         if role == "resolution" and any(w in message_text.lower() for w in ["fixed", "resolved", "closed", "done"]):
    #             ok = close_issue_in_datastore(issue_id, timestamp_ms)
    #             # Also remove from in-memory cache if present
    #             if ok:
    #                 if issue_id in open_issues:
    #                     del open_issues[issue_id]
    #                 print(f"✅ Closed issue: {issue_id}")
    #             else:
    #                 print("failed to close the issue in DS ... DOT")
    #     else:
    #         print(f"No open issue found to link this {role} message.")

    elif role in ["discussion", "resolution"]:
        issue_id = None

        # Link to latest open issue (simple and robust)
        latest_open = get_latest_open_issue_for_conversation(conversation_id)
        if latest_open:
            issue_id = latest_open.get("issue_id")
            print(f"🔗 Linked {role} to latest open issue: {issue_id}")
        else:
            print(f"No open issue in DS for conversation {conversation_id}; {role} unlinked.")

        # If this is a resolution and text clearly says it is fixed/closed, close the issue
        # If this is a resolution, generate summary and close issue
        if role == "resolution" and issue_id:
            messages = fetch_messages_by_issue_id(issue_id)
            summary = summarize_resolution_with_llm(messages)
            ok = store_resolution_summary(issue_id, summary, timestamp_ms)
            if ok:
                print(f"✅ Closed issue with summary: {issue_id}")
            else:
                print(f"❌ Failed to store resolution summary: {issue_id}")




        # 4. Store in Data Store
        row_id = insert_message_into_datastore(
            conversation_id, message_id, sender_id, timestamp_ms,
            message_text, role, category, severity, issue_id
        )
        
        # 5. Store in Qdrant
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


def fetch_all_issues():
    """Fetch ALL issues (Open + Resolved), sorted by resolved_at/open_at DESC"""
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

        # Sort by resolved_at (if exists) or opened_at, newest first
        def sort_key(issue):
            resolved = int(issue.get("resolved_at", 0))
            opened = int(issue.get("opened_at", 0))
            return max(resolved, opened)

        all_rows.sort(key=sort_key, reverse=True)
        return all_rows

    except Exception as e:
        print("fetch_all_issues exception:", e)
        return []



# ---------- Command Handlers ----------

def handle_search_command(query: str, chat: dict, user: dict):
    """Search incidents (Open + Resolved) by semantic similarity"""
    print(f"🔍 Searching incidents for: {query}")
    
    # 1. Embed query and search incidents
    q_emb = embed_text(query)
    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=10,  # Get more to rank better
    )
    
    if not hits:
        return jsonify({"text": f"No incidents found for `{query}`."})
    
    # 2. Get unique issue_ids from top hits, ranked by score
    issue_scores = {}
    for hit in hits:
        if hit.payload and (iid := hit.payload.get("issue_id")):
            issue_scores.setdefault(iid, 0)
            issue_scores[iid] = max(issue_scores[iid], hit.score)
    
    if not issue_scores:
        return jsonify({"text": "No incidents found."})
    
    # 3. Fetch ALL issues and filter by our scored issue_ids
    all_issues = fetch_all_issues()
    scored_issues = []
    for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True):
        issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
        if issue:
            issue["search_score"] = score
            scored_issues.append(issue)
    
    if not scored_issues:
        return jsonify({"text": "No issue details found."})
    
    # 4. Format beautiful results (top 5)
    lines = [f"🔍 **Search Results for: `{query}`** 📋\n━━━━━━━━━━━━━━━━━━━━━━━\n"]
    
    for idx, issue in enumerate(scored_issues[:5], 1):
        issue_id = issue.get("issue_id", "")[:12] + "..."
        title = issue.get("title", "")[:80]
        category = issue.get("category", "other")
        severity = issue.get("severity", "low")
        status = issue.get("status", "Open")
        
        # Dates
        opened_ts = int(issue.get("opened_at", 0))
        opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_ts else "N/A"
        
        resolved_ts = int(issue.get("resolved_at", 0))
        resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_ts else "—"
        
        # Resolution summary
        resolution_summary = issue.get("resolution_summary", "No summary")
        if status.lower() == "open":
            resolution_summary = "🟡 Open (no resolution yet)"
        
        lines.extend([
            f"**#{idx} {title}**  {severity.upper()}",
            f"🆔 `{issue_id}` | 📦 {category} | 📊 Score: {issue.get('search_score', 0):.2f}",
            f"📅 Opened: {opened_str} | ✅ {status} at: {resolved_str}",
            f"🔧 **Resolution:** {resolution_summary}",
            "━━━━━━━━━━━━━━━━━━━━━━━"
        ])
    
    return jsonify({"text": "\n".join(lines)})


# def handle_search_command(query: str, chat: dict, user: dict):
#     """
#     Search for past issues (not individual messages).
#     Show incident + resolution summary + metadata.
#     """
#     print(f"Searching issues for: {query!r}")
#     q_emb = embed_text(query)
    
#     # Search incidents only
#     q_filter = Filter(
#         must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
#     )
    
#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=q_emb,
#         query_filter=q_filter,
#         limit=5,
#     )
    
#     if not hits:
#         return jsonify({"text": f"No similar incidents found for `{query}`."})
    
#     # Group by issue_id
#     issue_ids = list({h.payload.get("issue_id") for h in hits if h.payload and h.payload.get("issue_id")})
    
#     if not issue_ids:
#         return jsonify({"text": "No past issues found."})
    
#     lines = [f"*Past incidents similar to:* `{query}`\n"]
    
#     for idx, issue_id in enumerate(issue_ids[:3], 1):
#         # Fetch all messages for this issue
#         messages = fetch_messages_by_issue_id(issue_id)
#         if not messages:
#             continue
        
#         # Find incident message
#         incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
#         if not incident_msg:
#             continue
        
#         title = incident_msg.get("message_text", "")[:100]
#         category = incident_msg.get("category", "other")
#         severity = incident_msg.get("severity", "low")
        
#         # Check if resolved
#         resolution_msgs = [m for m in messages if m.get("role") == "resolution"]
#         if resolution_msgs:
#             resolution_summary = summarize_resolution_with_llm(messages)
#             resolved_at_ts = max([m.get("time_stamp", 0) for m in resolution_msgs])
#             resolved_at = datetime.fromtimestamp(resolved_at_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
#             status = "Resolved"
#         else:
#             resolution_summary = "Open (no resolution yet)"
#             resolved_at = "—"
#             status = "Open"
        
#         lines.append(f"*#{idx}. {title}*")
#         lines.append(f"Category: {category} | Severity: {severity} | Status: {status}")
#         lines.append(f"Resolution: {resolution_summary}")
#         if resolved_at != "—":
#             lines.append(f"Resolved at: {resolved_at}")
#         lines.append("")  # blank line
    
#     return jsonify({"text": "\n".join(lines)})


# def handle_latest_issues(chat: dict, user: dict):
#     """
#     Show currently open issues.
#     """
#     issues = fetch_open_issues()
    
#     if not issues:
#         return jsonify({"text": "No open issues at the moment."})
    
#     lines = ["*Latest Open Issues:*\n"]
    
#     for idx, issue in enumerate(issues[:5], 1):
#         issue_id = issue.get("issue_id", "")
#         title = issue.get("title", "")
#         category = issue.get("category", "other")
#         severity = issue.get("severity", "low")
#         source = issue.get("source", "Cliq")
#         opened_at_ts = issue.get("opened_at", 0)
#         opened_at = datetime.fromtimestamp(opened_at_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        
#         lines.append(f"*#{idx}. {title}*")
#         lines.append(f"ID: {issue_id[:12]}...")
#         lines.append(f"Source: {source} | Category: {category} | Severity: {severity}")
#         lines.append(f"Opened at: {opened_at}")
#         lines.append("")
    
#     return jsonify({"text": "\n".join(lines)})

def handle_latest_issues(chat: dict, user: dict):
    issues = fetch_open_issues()
    
    if not issues:
        return jsonify({"text": "No open issues at the moment."})
    
    lines = ["*Latest Open Issues:*\n"]
    
    for idx, issue in enumerate(issues[:5], 1):
        issue_id = issue.get("issue_id", "")
        title = issue.get("title", "")
        category = issue.get("category", "other")
        severity = issue.get("severity", "low")
        source = issue.get("source", "Cliq")
        
        opened_at_raw = issue.get("opened_at", 0)
        try:
            opened_at_ms = int(opened_at_raw)
        except (TypeError, ValueError):
            opened_at_ms = 0
        
        if opened_at_ms > 0:
            opened_at = datetime.fromtimestamp(opened_at_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        else:
            opened_at = "N/A"
        
        lines.append(f"*#{idx}. {title}*")
        lines.append(f"ID: {issue_id[:12]}...")
        lines.append(f"Source: {source} | Category: {category} | Severity: {severity}")
        lines.append(f"Opened at: {opened_at}")
        lines.append("")
    
    return jsonify({"text": "\n".join(lines)})

# def handle_latest_issues(chat: dict, user: dict):
#     """
#     Show currently open issues as a Cliq message card with buttons
#     to view conversations related to each issue.
#     """
#     issues = fetch_open_issues()
#     if not issues:
#         return jsonify({"text": "No open issues at the moment."})

#     # Build a card with one section per issue, each with a button
#     card = {
#         "text": "Latest Open Issues",
#         "attachments": []
#     }

#     for idx, issue in enumerate(issues[:5], 1):
#         issue_id = issue.get("issue_id", "")
#         title = issue.get("title", "")
#         category = issue.get("category", "other")
#         severity = issue.get("severity", "low")
#         source = issue.get("source", "Cliq")

#         opened_at_raw = issue.get("opened_at", 0)
#         try:
#             opened_at_ms = int(opened_at_raw)
#         except (TypeError, ValueError):
#             opened_at_ms = 0

#         if opened_at_ms > 0:
#             opened_at = datetime.fromtimestamp(
#                 opened_at_ms / 1000, tz=timezone.utc
#             ).strftime("%Y-%m-%d %H:%M")
#         else:
#             opened_at = "N/A"

#         # Section text
#         section_text = (
#             f"*#{idx}. {title}*\n"
#             f"ID: `{issue_id}`\n"
#             f"Source: {source} | Category: {category} | Severity: {severity}\n"
#             f"Opened at: {opened_at}"
#         )

#         # Button that triggers @workspace-vita issue_conversations <issue_id>
#         button_label = "View conversations"
#         button_command = f"@{BOT_NAME} issue_conversations {issue_id}"

#         card["attachments"].append({
#             "color": "#36A64F",
#             "title": f"Issue #{idx}",
#             "text": section_text,
#             "actions": [
#                 {
#                     "type": "button",
#                     "text": button_label,
#                     "name": "view_conversations",
#                     "value": button_command
#                 }
#             ]
#         })

#     # Cliq understands message card JSON via "card" key
#     return jsonify({"card": card})

def handle_issue_conversations(issue_id: str, chat: dict, user: dict):
    """
    Show all conversations (messages) related to a particular issue_id.
    """
    if not issue_id:
        return jsonify({"text": "Usage: issue_conversations <issue_id>"})

    # Fetch messages linked to issue_id
    messages = fetch_messages_by_issue_id(issue_id)
    if not messages:
        return jsonify({"text": f"No messages found for issue `{issue_id}`."})

    # Sort by time_stamp ascending
    try:
        messages.sort(key=lambda m: int(m.get("time_stamp", 0)))
    except Exception:
        pass

    # Build a readable thread summary
    lines = [f"*Conversations for issue:* `{issue_id}`\n"]
    for m in messages[:30]:  # cap at 30 messages for brevity
        ts_raw = m.get("time_stamp", 0)
        try:
            ts_dt = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts_str = "N/A"

        role = m.get("role", "discussion")
        sender = m.get("sender_id", "")
        text = m.get("message_text", "")
        lines.append(f"- `{ts_str}` [{role}] {sender}: {text}")

    return jsonify({"text": "\n".join(lines)})




def handle_issue_details(incident_id: str, chat: dict, user: dict):
    return jsonify({"text": f"Details for issue `{incident_id}` coming soon."})


# ---------- /bot/command ----------

@app.route('/bot/command', methods=['POST'])
def bot_command():
    form_data = request.form.to_dict()
    print("==== /bot/command hit ====")

    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    command_text = form_data.get('commandText', '').strip()

    try:
        user = json.loads(user_raw)
        chat = json.loads(chat_raw)
    except Exception as e:
        return jsonify({"text": "Error parsing command."})

    tokens = command_text.split()
    if not tokens:
        return jsonify({"text": "Usage: search <query>, latest_issues, or issue_conversations <issue_id>"} )

    cmd = tokens[0].lower()
    args = " ".join(tokens[1:]).strip()

    if cmd == "search":
        if not args:
            return jsonify({"text": "Usage: search <query>"})
        return handle_search_command(args, chat, user)

    if cmd in ("latest_issues", "issues"):
        return handle_latest_issues(chat, user)

    if cmd == "issue_conversations":
        return handle_issue_conversations(args, chat, user)

    if cmd == "issue" and args:
        return handle_issue_details(args, chat, user)

    return jsonify({"text": f"Unknown command. Try: search, latest_issues, issue_conversations <issue_id>."})

# @app.route('/bot/command', methods=['POST'])
# def bot_command():
#     form_data = request.form.to_dict()
#     print("==== /bot/command hit ====")
    
#     user_raw = form_data.get('user')
#     chat_raw = form_data.get('chat')
#     command_text = form_data.get('commandText', '').strip()
    
#     try:
#         user = json.loads(user_raw)
#         chat = json.loads(chat_raw)
#     except Exception as e:
#         return jsonify({"text": "Error parsing command."})
    
#     tokens = command_text.split()
#     if not tokens:
#         return jsonify({"text": "Usage: search <query> or latest_issues"})
    
#     cmd = tokens[0].lower()
#     args = " ".join(tokens[1:]).strip()
    
#     if cmd == "search":
#         if not args:
#             return jsonify({"text": "Usage: search <query>"})
#         return handle_search_command(args, chat, user)
    
#     if cmd in ("latest_issues", "issues"):
#         return handle_latest_issues(chat, user)
    
#     if cmd == "issue" and args:
#         return handle_issue_details(args, chat, user)
    
#     return jsonify({"text": f"Unknown command. Try: search, latest_issues."})


# ========= OAuth (unchanged) =========

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
        return f"Failed to get access token", 400
    
    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful!"


# ========= Producer: Deluge → Signals =========

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
            resp = requests.post(
                SIGNALS_EVENT_URL,
                headers={"Content-Type": "application/json"},
                json=event_payload
            )
            print("Signals response:", resp.status_code)
        except Exception as e:
            print("Error publishing to Signals:", e)
    
    return jsonify({"status": "ok"})


# ========= Consumer: Signals → Indexing =========
@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    
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
        return jsonify({"status": "ignored"}), 200
    
    try:
        raw = json.loads(raw_str)
        user = json.loads(user_str)
        chat = json.loads(chat_str)
    except Exception as e:
        return jsonify({"status": "parse_error"}), 200
    
    try:
        message_text = raw["message"]["content"]["text"]
        message_id = raw["message"]["id"]
        timestamp_ms = raw["time"]
    except KeyError:
        return jsonify({"status": "bad_raw"}), 200
    
    # Skip bot commands - check for bot mention in multiple formats
    txt_lower = message_text.lower()
    
    # Skip if message contains bot mention (@workspace-vita or {@b-...})
    if "@workspace-vita" in txt_lower or "{@b-" in message_text:
        print(f"Skipping bot command from indexing: {message_text}")
        return jsonify({"status": "command_skipped"}), 200
    
    sender_id = user.get("zoho_user_id") or user.get("id")
    conversation_id = chat.get("id")
    
    print(f"📨 {message_text!r}")
    
    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)
    
    return jsonify({"status": "processed"})


# @app.route('/signals/consume', methods=['POST'])
# def signals_consume():
#     payload = request.get_json()
#     print("==== Signals delivered queued event ====")
    
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
#         return jsonify({"status": "ignored"}), 200
    
#     try:
#         raw = json.loads(raw_str)
#         user = json.loads(user_str)
#         chat = json.loads(chat_str)
#     except Exception as e:
#         return jsonify({"status": "parse_error"}), 200
    
#     try:
#         message_text = raw["message"]["content"]["text"]
#         message_id = raw["message"]["id"]
#         timestamp_ms = raw["time"]
#     except KeyError:
#         return jsonify({"status": "bad_raw"}), 200
    
#     # Skip bot commands
#     txt_lower = message_text.lower()
#     if f"@{BOT_NAME}" in txt_lower or "{@b-" in txt_lower:
#         print("Skipping bot command from indexing")
#         return jsonify({"status": "command_skipped"}), 200
    
#     sender_id = user.get("zoho_user_id") or user.get("id")
#     conversation_id = chat.get("id")
    
#     print(f"📨 {message_text!r}")
    
#     index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)
    
#     return jsonify({"status": "processed"})



##
@app.route('/search_incidents_card_json', methods=['GET'])
def search_incidents_card_json():
    """Returns search results formatted for Cliq cards"""
    query = request.args.get("query", "").strip()
    if not query:
        return jsonify({"error": "No query provided"})
    
    print(f"🔍 Card search for: {query}")
    
    # 1. Embed query and search
    q_emb = embed_text(query)
    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=10,
    )
    
    if not hits:
        return jsonify({"results": []})
    
    # 2. Get scored issue_ids
    issue_scores = {}
    for hit in hits:
        if hit.payload and (iid := hit.payload.get("issue_id")):
            issue_scores.setdefault(iid, 0)
            issue_scores[iid] = max(issue_scores[iid], hit.score)
    
    # 3. Fetch ALL issues and filter
    all_issues = fetch_all_issues()
    results = []
    
    for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:5]:
        issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
        if not issue:
            continue
        
        # Format dates
        opened_ts = int(issue.get("opened_at", 0))
        opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if opened_ts else "N/A"
        
        resolved_ts = int(issue.get("resolved_at", 0))
        resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if resolved_ts else "Not resolved"
        
        status = issue.get("status", "Open")
        
        # Get resolution summary
        resolution_summary = issue.get("resolution_summary", "")
        if not resolution_summary or status.lower() == "open":
            resolution_summary = "🟡 Still open - no resolution yet"
        
        results.append({
            "issue_id": issue.get("issue_id", ""),
            "title": issue.get("title", "")[:100],
            "category": issue.get("category", "other"),
            "severity": issue.get("severity", "low"),
            "status": status,
            "opened_at": opened_str,
            "resolved_at": resolved_str,
            "resolution_summary": resolution_summary[:200],  # Truncate for cards
            "score": round(score, 2)
        })
    
    return jsonify({
        "query": query,
        "results": results,
        "count": len(results)
    })


@app.route('/latest_issues_json', methods=['GET'])
def latest_issues_json():
    issues = fetch_open_issues()
    out = []
    for issue in issues:
        opened_at_raw = issue.get("opened_at", 0)
        try:
            opened_at_ms = int(opened_at_raw)
            opened_at_str = datetime.fromtimestamp(
                opened_at_ms / 1000, tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M")
        except Exception:
            opened_at_ms = 0
            opened_at_str = "N/A"
        out.append({
            "issue_id": issue.get("issue_id", ""),
            "title": issue.get("title", ""),
            "category": issue.get("category", "other"),
            "severity": issue.get("severity", "low"),
            "opened_at": opened_at_ms,
            "opened_at_str": opened_at_str,
        })
    return jsonify({"issues": out})

@app.route('/issue_conversations_json', methods=['GET'])
def issue_conversations_json():
    issue_id = request.args.get("issue_id", "").strip()
    if not issue_id:
        return jsonify({"messages": []})

    messages = fetch_messages_by_issue_id(issue_id)
    try:
        messages.sort(key=lambda m: int(m.get("time_stamp", 0)))
    except Exception:
        pass

    out = []
    for m in messages:
        ts_raw = m.get("time_stamp", 0)
        try:
            ts_dt = datetime.fromtimestamp(int(ts_raw) / 1000, tz=timezone.utc)
            ts_str = ts_dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            ts_str = "N/A"
        out.append({
            "time_str": ts_str,
            "role": m.get("role", "discussion"),
            "sender_id": m.get("sender_id", ""),
            "message_text": m.get("message_text", ""),
        })
    return jsonify({"messages": out})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
