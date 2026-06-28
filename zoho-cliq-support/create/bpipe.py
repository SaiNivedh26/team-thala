from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect
import requests
import os
import json
from datetime import datetime, timezone
import uuid
import tempfile
import base64
import threading
import time

# ✅ ADD: APScheduler for background token refresh
from apscheduler.schedulers.background import BackgroundScheduler

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

# ========= OAuth Config =========
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8000/oauth/callback'
AUTH_URL = 'https://accounts.zoho.com/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.com/oauth/v2/token'

# ========= Catalyst Config =========
CATALYST_PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")
CATALYST_ORG_ID = os.getenv("CATALYST_ORG_ID")
CONVERSATIONS_TABLE = "conversations"
ISSUES_TABLE = "issues"

# ========= Signals =========
SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')

# ========= Qdrant / Gemini =========
QDRANT_URL = os.getenv("QDRANT_URL")
QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_COLLECTION = "messages_vec"
BUCKET_URL = os.getenv("BUCKET_URL")

qdrant = QdrantClient(url=QDRANT_URL, api_key=QDRANT_API_KEY)
genai_client = genai.Client()

BOT_NAME = "workspace-vita"

# ✅ TOKEN MANAGER WITH AUTO REFRESH
class TokenManager:
    def __init__(self):
        self.access_token = os.getenv("CATALYST_TOKEN")
        self.refresh_token = os.getenv("CATALYST_REFRESH_TOKEN")  # Add this to .env
        self.token_expiry = None
        self.lock = threading.Lock()
        
        print("🔐 Token Manager initialized")
        if self.refresh_token:
            print("✅ Refresh token available - auto-refresh enabled")
        else:
            print("⚠️ No refresh token - manual token updates required")
    
    def get_token(self):
        """Get current access token"""
        with self.lock:
            return self.access_token
    
    def refresh_access_token(self):
        """Refresh access token using refresh token"""
        if not self.refresh_token:
            print("❌ No refresh token available, cannot auto-refresh")
            return False
        
        print("🔄 Refreshing access token...")
        
        try:
            data = {
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token,
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            }
            
            resp = requests.post(TOKEN_URL, data=data, timeout=30)
            
            if resp.status_code == 200:
                token_json = resp.json()
                
                with self.lock:
                    self.access_token = token_json.get("access_token")
                    
                    # Update refresh token if new one provided
                    if "refresh_token" in token_json:
                        self.refresh_token = token_json["refresh_token"]
                    
                    # Set expiry (tokens typically last 1 hour)
                    self.token_expiry = time.time() + 3600
                
                print(f"✅ Token refreshed successfully")
                print(f"   New token: {self.access_token[:20]}...")
                print(f"   Expires in: 60 minutes")
                
                return True
            else:
                print(f"❌ Token refresh failed: {resp.status_code}")
                print(f"   Response: {resp.text[:200]}")
                return False
                
        except Exception as e:
            print(f"❌ Token refresh exception: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def start_auto_refresh(self):
        """Start background scheduler for automatic token refresh"""
        if not self.refresh_token:
            print("⚠️ Auto-refresh disabled (no refresh token)")
            return
        
        scheduler = BackgroundScheduler()
        
        # Refresh every 55 minutes (5 min buffer before expiry)
        scheduler.add_job(
            func=self.refresh_access_token,
            trigger="interval",
            minutes=55,
            id="token_refresh",
            name="Auto Token Refresh",
            replace_existing=True
        )
        
        scheduler.start()
        print("✅ Auto token refresh scheduled (every 55 minutes)")
        
        # Do first refresh immediately if token is old
        if not self.token_expiry or time.time() > self.token_expiry - 300:
            print("🔄 Performing initial token refresh...")
            self.refresh_access_token()

# ✅ Initialize Token Manager
token_manager = TokenManager()

# ✅ Global getter for token (replaces CATALYST_TOKEN)
def get_catalyst_token():
    return token_manager.get_token()

# For backward compatibility, create a property-like access
CATALYST_TOKEN = property(lambda self: token_manager.get_token())


# In-memory issue tracker (for fast access)
open_issues = {}  # {issue_id: {"opened_at": ts, "title": str, "category": str, "severity": str}}
@app.route('/quick_fix_past', methods=['GET'])
def quick_fix_past():
    """Find similar resolved incidents for quick fixes"""
    issue_id = request.args.get('issue_id', '').strip()
    
    if not issue_id:
        return jsonify({"error": "No issue_id provided"}), 400
    
    print(f"\n🔍 QUICK FIX - PAST FIXES for {issue_id[:12]}...")
    
    # Get the issue
    all_issues = fetch_all_issues()
    current_issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
    
    if not current_issue:
        return jsonify({"error": "Issue not found"}), 404
    
    # Get incident message
    messages = fetch_messages_by_issue_id(issue_id)
    incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
    
    if not incident_msg:
        return jsonify({"error": "No incident message found"}), 404
    
    incident_text = incident_msg.get("message_text", "")
    
    # Embed incident
    incident_emb = embed_text(incident_text)
    
    # Search for similar RESOLVED incidents
    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=incident_emb,
        query_filter=q_filter,
        limit=20,
    )
    
    # Filter for resolved issues only (exclude current issue)
    resolved_issues = [i for i in all_issues if i.get("status", "").lower() == "resolved"]
    resolved_ids = {i.get("issue_id") for i in resolved_issues}
    
    similar_resolved = {}
    for hit in hits:
        if not hit.payload:
            continue
        candidate_id = hit.payload.get("issue_id")
        if candidate_id and candidate_id != issue_id and candidate_id in resolved_ids:
            if hit.score >= 0.60:  # Similarity threshold
                similar_resolved.setdefault(candidate_id, 0)
                similar_resolved[candidate_id] = max(similar_resolved[candidate_id], hit.score)
    
    # Build results
    results = []
    for res_issue_id, score in sorted(similar_resolved.items(), key=lambda x: x[1], reverse=True)[:3]:
        res_issue = next((i for i in resolved_issues if i.get("issue_id") == res_issue_id), None)
        if not res_issue:
            continue
        
        resolution = res_issue.get("resolution_summary", "No details")
        title = res_issue.get("title", "Untitled")[:80]
        
        results.append({
            "issue_id": res_issue_id,
            "title": title,
            "resolution": resolution,
            "similarity": round(score, 2)
        })
    
    print(f"✅ Found {len(results)} similar resolved issues")
    return jsonify({"results": results, "count": len(results)})

@app.route('/quick_fix_web', methods=['GET'])
def quick_fix_web():
    """Generate search query using LLM and search web for fixes"""
    issue_id = request.args.get('issue_id', '').strip()
    
    if not issue_id:
        return jsonify({"error": "No issue_id provided"}), 400
    
    print(f"\n🌐 QUICK FIX - WEB SEARCH for {issue_id[:12]}...")
    
    # Get messages for this issue
    messages = fetch_messages_by_issue_id(issue_id)
    
    if not messages:
        return jsonify({"error": "No messages found"}), 404
    
    # Build context from incident + discussions
    incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
    discussions = [m.get("message_text", "") for m in messages if m.get("role") == "discussion"][:3]
    
    if not incident_msg:
        return jsonify({"error": "No incident found"}), 404
    
    incident_text = incident_msg.get("message_text", "")
    context = f"Incident: {incident_text}\n"
    
    if discussions:
        context += f"Context: {'; '.join(discussions)}"
    
    print(f"📝 Context: {context[:200]}...")
    
    # ✅ Use LLM to generate search query
    search_query = generate_search_query_with_llm(context)
    
    if not search_query:
        return jsonify({"error": "Failed to generate search query"}), 500
    
    print(f"🔍 Generated query: {search_query}")
    
    # Call Tavily search
    try:
        import urllib.parse
        encoded_query = urllib.parse.quote(search_query)
        tavily_url = f"https://tavily-search-907381267.development.catalystserverless.com/server/tavily_search_function/execute?query={encoded_query}"
        
        resp = requests.get(tavily_url, timeout=30)
        
        if resp.status_code == 200:
            data = resp.json()
            output = json.loads(data.get("output", "{}"))
            
            results = output.get("data", [])
            print(f"✅ Tavily returned {len(results)} results")
            
            return jsonify({
                "query": search_query,
                "results": results[:5],
                "count": len(results)
            })
        else:
            return jsonify({"error": "Tavily search failed"}), 500
            
    except Exception as e:
        print(f"❌ Web search error: {e}")
        return jsonify({"error": str(e)}), 500


def generate_search_query_with_llm(context: str) -> str:
    """Use LLM to craft perfect search query from incident context"""
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    prompt = f"""Generate a concise web search query to find solutions for this IT incident.

Context:
{context[:500]}

Return ONLY the search query (5-10 words), no explanation. Focus on the error/problem and technology.

Examples:
- "API Gateway connection refused AWS"
- "production database timeout PostgreSQL fix"
- "payment gateway 500 error resolution"

Query:"""
    
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Generate concise technical search queries for finding solutions to IT issues.",
        "temperature": 0.3,
        "max_tokens": 50,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()
            
            output = None
            if "data" in result and "output_text" in result["data"]:
                output = result["data"]["output_text"]
            elif "output_text" in result:
                output = result["output_text"]
            elif "response" in result:
                output = result["response"]
            
            if output:
                query = output.strip().strip('"').strip("'")
                
                # Clean up
                for prefix in ["Query:", "Search:"]:
                    if query.startswith(prefix):
                        query = query[len(prefix):].strip()
                
                return query
        
        # Fallback: extract key terms
        print(f"⚠️ LLM query generation failed, using fallback")
        words = context.split()[:10]
        return " ".join(words)
        
    except Exception as e:
        print(f"❌ Query generation error: {e}")
        # Fallback
        words = context.split()[:10]
        return " ".join(words)



def check_resolution_specificity(resolution_text: str) -> dict:
    """Use LLM to determine if resolution is vague/generic or specific"""
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    prompt = f"""Analyze this resolution message and determine if it's VAGUE or SPECIFIC.

Resolution: "{resolution_text}"

VAGUE = Generic statements without mentioning what was fixed (e.g., "fixed", "done", "working now", "issue resolved")
SPECIFIC = Mentions what was done or what issue was resolved (e.g., "restarted payment gateway", "fixed database timeout", "resolved API connection issue")

Return ONLY this JSON (no extra text):
{{"specificity": "vague|specific"}}

JSON:"""
    
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Classify resolution messages as vague or specific. Return ONLY valid JSON.",
        "temperature": 0.1,
        "max_tokens": 50,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()
            
            # Get output
            output_text = None
            if "data" in result and "output_text" in result["data"]:
                output_text = result["data"]["output_text"]
            elif "output_text" in result:
                output_text = result["output_text"]
            elif "response" in result:
                output_text = result["response"]
            
            if output_text:
                # Clean and parse JSON
                output_text = output_text.strip()
                if "{" in output_text and "}" in output_text:
                    start = output_text.index("{")
                    end = output_text.rindex("}") + 1
                    json_str = output_text[start:end]
                else:
                    json_str = output_text
                
                parsed = json.loads(json_str)
                specificity = parsed.get("specificity", "specific")
                
                print(f"✅ LLM specificity check: {specificity}")
                return {"specificity": specificity}
        
        # Fallback: assume specific (safer to use similarity)
        print(f"⚠️ LLM specificity check failed, assuming specific")
        return {"specificity": "specific"}
        
    except Exception as e:
        print(f"❌ Specificity check error: {e}")
        # Fallback: assume specific
        return {"specificity": "specific"}







def extract_incident_title_from_analysis(analysis: str) -> str:
    """Extract a concise incident title from vision analysis using LLM"""
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    prompt = f"""Extract the main incident from this analysis as a short title (max 10 words).

Analysis: "{analysis[:300]}"

Return ONLY the incident title, no extra text. Examples:
- "Production database is down"
- "Payment gateway connection refused"
- "API timeout errors"

Title:"""
    
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Extract concise incident titles. Return only the title, no formatting.",
        "temperature": 0.2,
        "max_tokens": 50,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        
        if resp.status_code == 200:
            result = resp.json()
            
            # Get output
            output = None
            if "data" in result and "output_text" in result["data"]:
                output = result["data"]["output_text"]
            elif "output_text" in result:
                output = result["output_text"]
            elif "response" in result:
                output = result["response"]
            
            if output:
                title = output.strip().strip('"').strip("'")
                
                # Clean up common prefixes
                for prefix in ["Title:", "Incident:", "Issue:"]:
                    if title.startswith(prefix):
                        title = title[len(prefix):].strip()
                
                # Limit length
                if len(title) > 80:
                    title = title[:77] + "..."
                
                print(f"✅ Extracted title: {title}")
                return title
        
        # Fallback: use first sentence of analysis
        print(f"⚠️ LLM title extraction failed, using fallback")
        first_sentence = analysis.split('.')[0][:80]
        return first_sentence if first_sentence else "Incident detected from image"
        
    except Exception as e:
        print(f"❌ Title extraction error: {e}")
        # Fallback
        first_sentence = analysis.split('.')[0][:80]
        return first_sentence if first_sentence else "Incident detected from image"




# ---------- LLM Classification ----------
# def classify_message_llm(text: str) -> dict:
#     url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID,
#     }
    
#     # ✅ SHORTER BUT CLEARER PROMPT
#     prompt = f"""Classify this message:

# "{text}"

# Return ONLY JSON:
# {{"role": "incident|discussion|resolution", "category": "database|cache|auth|network|security|deployment|other", "severity": "low|medium|high"}}

# Role rules:
# • incident = reports CURRENT broken/failing system (e.g. "X is down", "getting errors")
# • resolution = states problem is ALREADY fixed (e.g. "fixed", "working now", "resolved")
# • discussion = investigating, planning, explaining, or any other message

# Category: database, cache, auth, network, security, deployment, or other
# Severity: low, medium, or high

# Examples:
# "API is down" → incident, network, high
# "let me check logs" → discussion, other, low
# "fixed the bug" → resolution, other, medium

# JSON:"""

#     data = {
#         "prompt": prompt,
#         "model": "crm-di-qwen_text_14b-fp8-it",
#         "system_prompt": "Classify messages strictly: 'incident'=currently broken, 'resolution'=already fixed, 'discussion'=everything else. Return valid JSON only.",
#         "top_p": 0.7,
#         "top_k": 30,
#         "temperature": 0.1,
#         "max_tokens": 100,
#     }

#     try:
#         resp = requests.post(url, json=data, headers=headers, timeout=30)
        
#         if resp.status_code != 200:
#             print(f"❌ LLM error {resp.status_code}: {resp.text}")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         result = resp.json()
        
#         # Extract output
#         output_text = None
#         if "data" in result and "output_text" in result["data"]:
#             output_text = result["data"]["output_text"]
#         elif "output_text" in result:
#             output_text = result["output_text"]
#         elif "response" in result:
#             output_text = result["response"]
        
#         if not output_text:
#             print("❌ No output from LLM")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         # Extract JSON
#         output_text = output_text.strip()
#         if "{" in output_text and "}" in output_text:
#             start = output_text.index("{")
#             end = output_text.rindex("}") + 1
#             json_str = output_text[start:end]
#         else:
#             json_str = output_text
        
#         classification = json.loads(json_str)
        
#         # Validate
#         if "role" not in classification or "category" not in classification or "severity" not in classification:
#             print(f"❌ Invalid classification: {classification}")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         # Validate role value
#         if classification["role"] not in ["incident", "discussion", "resolution"]:
#             print(f"❌ Invalid role: {classification['role']}")
#             return {"role": "discussion", "category": "other", "severity": "low"}
        
#         print(f"✅ Parsed classification: {classification}")
#         return classification
        
#     except json.JSONDecodeError as e:
#         print(f"❌ JSON parse error: {e}")
#         return {"role": "discussion", "category": "other", "severity": "low"}
#     except Exception as e:
#         print(f"❌ Classification error: {e}")
#         import traceback
#         traceback.print_exc()
#         return {"role": "discussion", "category": "other", "severity": "low"}


def get_issue_id_from_last_message(conversation_id: str):
    """Get issue_id from the most recent message in this conversation"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return None

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }

    try:
        # Fetch messages for this conversation
        resp = requests.get(f"{url}?max_rows=100", headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        
        rows = resp.json().get("data", [])
        
        # Filter by conversation_id
        conv_messages = [r for r in rows if r.get("conversation_id") == conversation_id]
        
        if not conv_messages:
            print(f"❌ No previous messages in conversation {conversation_id}")
            return None
        
        # Sort by timestamp descending (newest first)
        conv_messages.sort(key=lambda r: int(r.get("time_stamp", 0)), reverse=True)
        
        # Get issue_id from most recent message
        for msg in conv_messages:
            issue_id = msg.get("issue_id")
            if issue_id and issue_id != "":
                print(f"📎 Found issue_id from previous message: {issue_id[:12]}")
                return issue_id
        
        print(f"❌ No issue_id found in previous messages")
        return None
        
    except Exception as e:
        print(f"❌ get_issue_id_from_last_message exception: {e}")
        return None









def classify_message_llm(text: str) -> dict:
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    prompt = f"""Classify this engineering message:

Statement: "{text}"

Return ONLY this JSON (no extra text):
{{"role": "incident|discussion|resolution", "category": "database|cache|auth|network|security|deployment|other", "severity": "low|medium|high"}}

Rules:
- role:
  - "resolution" ONLY if the message explicitly states that a problem is ALREADY fixed, resolved, closed, working now, back to normal, or issue is gone. Examples: "fixed it", "issue resolved", "back to normal now", "working fine now" . sometimes people can discuss like i think i got the issue, this is due to so and so reasons, lemme do smthg else.. so these are purely discussions.. not resolutions. Resoulution comes in if they are purely confident about it, like this issue is fixed, back to normal.. similar. Think from perspective of IT Employees
  - "incident" if it reports an active problem, failure, error, outage, or something broken. Think from perspective of IT Employees
  - "discussion" for everything else including: suggestions ("let's try"), questions ("should we?"), plans ("we need to"), explanations, acknowledgments, or any chat that is NOT an active incident report or a completion statement. Think from perspective of IT Employees . There can be technical and casual convos as well. 
- category: database, cache, auth, network, security, deployment, or other
- severity: low, medium, or high

JSON:"""

    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "You classify engineering messages.Your decisions lead to important IT Decisions. So Be very careful.  Return ONLY valid JSON. Be strict: 'resolution' means the problem is ALREADY solved, not planned or being worked on.",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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

def fetch_all_issues():
    """Fetch ALL issues (Open + Resolved), sorted by recency"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return []

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
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
            print(f"DS fetch all issues: {resp.status_code}")
            if resp.status_code != 200:
                break

            body = resp.json()
            rows = body.get("data", [])
            all_rows.extend(rows)

            next_token = body.get("next_token")
            if not next_token:
                break

        # Sort by most recent (resolved_at if exists, else opened_at)
        def sort_key(issue):
            resolved = int(issue.get("resolved_at", 0))
            opened = int(issue.get("opened_at", 0))
            return max(resolved, opened)

        all_rows.sort(key=sort_key, reverse=True)
        print(f"✅ Fetched {len(all_rows)} total issues")
        return all_rows

    except Exception as e:
        print(f"fetch_all_issues exception: {e}")
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


def summarize_resolution_with_llm(messages: list, current_resolution_text: str = None) -> str:
    """Generate resolution summary using same pattern as classification"""
    if not messages and not current_resolution_text:
        return "No resolution details available."
    
    # Get relevant messages
    relevant = [m for m in messages if m.get("role") in ["incident", "discussion", "resolution"]]
    
    # Add current resolution if provided
    if current_resolution_text:
        relevant.append({
            "role": "resolution",
            "message_text": current_resolution_text
        })
    
    if not relevant:
        return "Issue resolved (no details recorded)."
    
    # Extract components
    incident = next((m.get("message_text", "") for m in relevant if m.get("role") == "incident"), "")
    discussions = [m.get("message_text", "") for m in relevant if m.get("role") == "discussion"][:3]
    resolutions = [m.get("message_text", "") for m in relevant if m.get("role") == "resolution"]
    
    if not resolutions:
        return "Resolved (no resolution message recorded)."
    
    # Build context (limit to avoid errors)
    incident_text = incident[:100] if incident else "Unknown issue"
    discussion_text = "; ".join(discussions)[:150] if discussions else "No investigation notes"
    resolution_text = "; ".join(resolutions)[:150]
    
    print(f"📝 Summary components: incident={len(incident_text)} chars, discussions={len(discussion_text)} chars, resolutions={len(resolution_text)} chars")
    
    # ✅ SAME URL AND HEADERS AS CLASSIFICATION
    url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    # ✅ SIMILAR PROMPT STRUCTURE TO CLASSIFICATION
    prompt = f"""Summarize this incident resolution in 1-2 sentences.

Problem: "{incident_text}"

Investigation: "{discussion_text}"

Resolution: "{resolution_text}"

Provide a concise summary of how the issue was resolved (what was done and the outcome).

Summary:"""
    
    # ✅ SAME DATA STRUCTURE AS CLASSIFICATION
    data = {
        "prompt": prompt,
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "You summarize incident resolutions clearly. Describe what was done to fix the problem and the outcome in 1-2 sentences.",
        "top_p": 0.8,
        "top_k": 40,
        "best_of": 1,
        "temperature": 0.3,
        "max_tokens": 150,
    }
    
    try:
        resp = requests.post(url, json=data, headers=headers, timeout=30)
        print(f"LLM Summary Status: {resp.status_code}")
        
        if resp.status_code != 200:
            print(f"LLM Summary Error Response: {resp.text[:200]}")
            # Fallback to resolution text
            return resolution_text
        
        result = resp.json()
        print(f"LLM Summary Full Response: {json.dumps(result, indent=2)}")
        
        # ✅ SAME RESPONSE PARSING AS CLASSIFICATION
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
        
        print(f"Extracted summary output_text: {output_text}")
        
        if not output_text:
            print("No output_text found in summary response")
            return resolution_text
        
        # Clean the output
        summary = output_text.strip()
        
        # Remove any leading/trailing quotes or markdown
        if summary.startswith('"') and summary.endswith('"'):
            summary = summary[1:-1]
        
        if len(summary) > 10:
            print(f"✅ Parsed summary: {summary[:100]}...")
            return summary
        else:
            print(f"⚠️ Summary too short, using fallback")
            return resolution_text
        
    except json.JSONDecodeError as e:
        print(f"JSON parse error in summary: {e}")
        return resolution_text
    except Exception as e:
        print(f"LLM summary exception: {e}")
        import traceback
        traceback.print_exc()
        return resolution_text




# ========== ATTACHMENT HANDLING FUNCTIONS ==========

def upload_to_stratus(local_path: str, object_key: str) -> str:
    """Upload file to Stratus and return URL"""
    try:
        with open(local_path, "rb") as f:
            file_content = f.read()
        
        url = f"{BUCKET_URL}/{object_key}"
        headers = {
            "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
            "compress": "false",
            "cache-control": "max-age=3600"
        }
        
        resp = requests.put(url, data=file_content, headers=headers, timeout=120)
        print(f"Stratus upload: {resp.status_code}")
        
        if resp.status_code in [200, 201, 204]:
            print(f"✓ Uploaded to Stratus: {object_key}")
            return url
        else:
            print(f"✗ Upload failed: {resp.text[:200]}")
            return None
            
    except Exception as e:
        print(f"❌ Stratus upload error: {e}")
        return None


def process_image_with_vision(local_path: str) -> str:
    """Analyze image using Qwen Vision model"""
    try:
        with open(local_path, "rb") as f:
            b64img = base64.b64encode(f.read()).decode()
        
        url = f"https://api.catalyst.zoho.com/quickml/v1/project/{CATALYST_PROJECT_ID}/vlm/chat"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {get_catalyst_token()}",
            "CATALYST-ORG": CATALYST_ORG_ID
        }
        
        data = {
            "prompt": "Analyze this image for IT incidents, errors, logs, or system issues. If you find any production problems, database issues, network errors, or service outages, describe them clearly. If it's just a casual image, say 'No incident detected'.",
            "model": "VL-Qwen2.5-7B",
            "images": [b64img],
            "system_prompt": "You are an IT incident analyzer. Be concise and factual.",
            "top_k": 50,
            "top_p": 0.9,
            "temperature": 0.7,
            "max_tokens": 500
        }
        
        resp = requests.post(url, json=data, headers=headers, timeout=90)
        print(f"Vision model: {resp.status_code}")
        
        if resp.status_code == 200:
            result = resp.json()
            output = result.get("data", {}).get("output_text", "") or result.get("response", "")
            print(f"Vision analysis: {output[:100]}...")
            return output.strip()
        else:
            print(f"Vision error: {resp.text[:200]}")
            return ""
            
    except Exception as e:
        print(f"❌ Vision model error: {e}")
        import traceback
        traceback.print_exc()
        return ""


def ocr_document(local_path: str, filename: str) -> str:
    """Run OCR on document/PDF and return extracted text"""
    try:
        url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/ml/ocr"
        headers = {
            "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}"
        }
        
        # Determine content type
        ext = filename.split(".")[-1].lower() if "." in filename else ""
        
        if ext == "pdf":
            content_type = "application/pdf"
        elif ext in ["jpg", "jpeg"]:
            content_type = "image/jpeg"
        elif ext == "png":
            content_type = "image/png"
        elif ext == "webp":
            content_type = "image/webp"
        elif ext in ["tiff", "tif"]:
            content_type = "image/tiff"
        elif ext == "bmp":
            content_type = "image/bmp"
        else:
            content_type = "application/octet-stream"
        
        print(f"📄 OCR for {filename} (type: {content_type}, size: {os.path.getsize(local_path)} bytes)")
        
        # ✅ FIELD NAME MUST BE "image" (not "file") per docs
        with open(local_path, "rb") as f:
            files = {
                "image": (filename, f, content_type)
            }
            data = {
                "language": "eng"
            }
            
            resp = requests.post(url, headers=headers, files=files, data=data, timeout=120)
        
        print(f"OCR response: {resp.status_code}")
        
        if resp.status_code == 200:
            result = resp.json()
            print(f"OCR result: {json.dumps(result, indent=2)[:500]}...")
            
            # ✅ CORRECT PATH: result -> data -> text
            if "data" in result and isinstance(result["data"], dict):
                text = result["data"].get("text", "")
            elif "extracted_text" in result:
                # Fallback for older format
                extracted_text = result.get("extracted_text", [])
                if isinstance(extracted_text, list):
                    text = " ".join(
                        block.get("text", "") if isinstance(block, dict) else str(block)
                        for block in extracted_text
                    )
                elif isinstance(extracted_text, str):
                    text = extracted_text
                else:
                    text = ""
            else:
                text = ""
            
            # Clean whitespace
            text = " ".join(text.split())
            
            print(f"✓ OCR extracted {len(text)} characters")
            
            if text:
                print(f"📝 First 100 chars: {text[:100]}")
            
            return text
        else:
            print(f"❌ OCR failed: {resp.status_code}")
            print(f"Response: {resp.text[:300]}")
            return ""
            
    except Exception as e:
        print(f"❌ OCR exception: {e}")
        import traceback
        traceback.print_exc()
        return ""



# def ocr_document(local_path: str, filename: str) -> str:
#     """Run OCR on document/PDF and return extracted text"""
#     try:
#         url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/ml/ocr"
#         headers = {
#             "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}"
#         }
        
#         # Determine content type
#         ext = filename.split(".")[-1].lower() if "." in filename else ""
        
#         if ext == "pdf":
#             content_type = "application/pdf"
#         elif ext in ["jpg", "jpeg"]:
#             content_type = "image/jpeg"
#         elif ext == "png":
#             content_type = "image/png"
#         else:
#             content_type = "application/octet-stream"
        
#         print(f"📄 OCR for {filename} (type: {content_type})")
        
#         with open(local_path, "rb") as f:
#             files = {
#                 "image": (filename, f, content_type)
#             }
#             data = {
#                 "language": "eng"
#             }
            
#             resp = requests.post(url, headers=headers, files=files, data=data, timeout=120)
        
#         print(f"OCR response: {resp.status_code}")
        
#         if resp.status_code == 200:
#             result = resp.json()
#             print(f"OCR result keys: {result.keys()}")
            
#             # Try different response formats
#             text_blocks = result.get("extracted_text", [])
            
#             if text_blocks:
#                 if isinstance(text_blocks, list):
#                     text = " ".join(block.get("text", "") for block in text_blocks if isinstance(block, dict) and block.get("text"))
#                 elif isinstance(text_blocks, str):
#                     text = text_blocks
#                 else:
#                     text = str(text_blocks)
#             else:
#                 # Try alternative key
#                 text = result.get("text", "")
            
#             print(f"OCR extracted {len(text)} characters")
            
#             if text:
#                 return text
#             else:
#                 print(f"⚠️ OCR returned empty. Full response: {result}")
#                 return ""
#         else:
#             print(f"OCR error: {resp.text[:200]}")
#             return ""
            
#     except Exception as e:
#         print(f"❌ OCR error: {e}")
#         import traceback
#         traceback.print_exc()
#         return ""


# def ocr_document(local_path: str, filename: str) -> str:
#     """Run OCR on document/PDF and return extracted text"""
#     try:
#         url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/ml/ocr"
#         headers = {
#             "Authorization": f"Zoho-oauthtoken {CATALYST_TOKEN}"
#         }
        
#         with open(local_path, "rb") as f:
#             files = {"image": (filename, f)}
#             data = {"language": "eng"}
            
#             resp = requests.post(url, headers=headers, files=files, data=data, timeout=120)
        
#         print(f"OCR: {resp.status_code}")
        
#         if resp.status_code == 200:
#             result = resp.json()
#             text_blocks = result.get("extracted_text", [])
#             text = " ".join(block.get("text", "") for block in text_blocks if block.get("text"))
#             print(f"OCR extracted {len(text)} characters")
#             return text
#         else:
#             print(f"OCR error: {resp.text[:200]}")
#             return ""
            
#     except Exception as e:
#         print(f"❌ OCR error: {e}")
#         import traceback
#         traceback.print_exc()
#         return ""


def process_attachment(attachment: dict, conversation_id: str, sender_id: str, timestamp_ms: int):
    """Process single attachment: download, upload to Stratus, analyze"""
    try:
        att_url = attachment.get("url")
        filename = attachment.get("name", f"attachment_{uuid.uuid4()}")
        file_id = attachment.get("id", str(uuid.uuid4()))
        
        if not att_url:
            print("⚠️ No attachment URL")
            return
        
        print(f"📎 Processing attachment: {filename}")
        
        # Determine file extension
        ext = filename.split(".")[-1].lower() if "." in filename else "bin"
        
        # Download to temp file
        temp_path = tempfile.mktemp(suffix=f".{ext}")
        
        try:
            # Stream download (handles large files)
            print(f"⬇️ Downloading: {att_url[:50]}...")
            resp = requests.get(att_url, stream=True, timeout=120)
            
            with open(temp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024*1024):  # 1MB chunks
                    if chunk:
                        f.write(chunk)
            
            file_size = os.path.getsize(temp_path)
            print(f"✓ Downloaded {file_size} bytes")
            
            # Upload to Stratus
            stratus_key = f"attachments/{file_id}_{filename}"
            stratus_url = upload_to_stratus(temp_path, stratus_key)
            
            if not stratus_url:
                print("⚠️ Stratus upload failed, skipping analysis")
                return
            
            # Process based on file type
            if ext in ("png", "jpg", "jpeg", "bmp", "webp", "gif"):
                # Image: Vision model
                print("🖼️ Processing image with Vision model...")
                analysis = process_image_with_vision(temp_path)
                
                if not analysis:
                    analysis = "Image uploaded (analysis unavailable)"
                
                # Check if incident detected
                analysis_lower = analysis.lower()
                is_incident = any(keyword in analysis_lower for keyword in 
                    ["error", "failed", "down", "outage", "critical", "production", 
                     "database", "timeout", "crash", "exception"])
                
                if is_incident and "no incident" not in analysis_lower:
                    # Create incident from image
                    print(f"🚨 Incident detected in image!")
                    message_text = f"[Image Analysis] {analysis}"
                    index_message(conversation_id, f"img_{file_id}", sender_id, timestamp_ms, message_text)
                else:
                    # Discussion
                    message_text = f"User shared image: {stratus_url}\n\nVision analysis: {analysis}"
                    insert_message_into_datastore(
                        conversation_id, f"img_{file_id}", sender_id, timestamp_ms,
                        message_text, "discussion", "other", "low", None
                    )
                    print(f"💬 Image stored as discussion")
            
            elif ext in ("pdf", "doc", "docx", "txt"):
                # Document: OCR
                print(f"📄 Processing document with OCR...")
                ocr_text = ocr_document(temp_path, filename)
                
                if not ocr_text:
                    ocr_text = "Document uploaded (OCR failed)"
                
                # Summarize with LLM
                summary_prompt = f"Summarize this document in 2-3 sentences: {ocr_text[:1000]}"
                summary_resp = classify_message_llm(summary_prompt)
                
                summary = f"Document '{filename}' uploaded. Category: {summary_resp.get('category', 'other')}. Content: {ocr_text[:200]}..."
                
                # Always discussion for documents
                message_text = f"User shared document: {stratus_url}\n\n{summary}"
                insert_message_into_datastore(
                    conversation_id, f"doc_{file_id}", sender_id, timestamp_ms,
                    message_text, "discussion", summary_resp.get("category", "other"), 
                    summary_resp.get("severity", "low"), None
                )
                print(f"📑 Document stored as discussion")
            
            else:
                # Other files
                message_text = f"User shared file: {filename} ({stratus_url})"
                insert_message_into_datastore(
                    conversation_id, f"file_{file_id}", sender_id, timestamp_ms,
                    message_text, "discussion", "other", "low", None
                )
                print(f"📦 File stored as discussion")
        
        finally:
            # Cleanup temp file
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"🗑️ Cleaned up temp file")
    
    except Exception as e:
        print(f"❌ Attachment processing error: {e}")
        import traceback
        traceback.print_exc()




# def summarize_resolution_with_llm(messages: list, current_resolution_text: str = None) -> str:
#     """Generate resolution summary from linked messages + current resolution"""
#     if not messages and not current_resolution_text:
#         return "No resolution details available."
    
#     # Get incident + discussion + resolution messages
#     relevant = [m for m in messages if m.get("role") in ["incident", "discussion", "resolution"]]
    
#     # Add current resolution if provided
#     if current_resolution_text:
#         relevant.append({
#             "role": "resolution",
#             "message_text": current_resolution_text
#         })
    
#     if not relevant:
#         return "Issue resolved (no details recorded)."
    
#     # Build CONCISE context
#     incident = next((m.get("message_text", "") for m in relevant if m.get("role") == "incident"), "")
#     resolutions = [m.get("message_text", "") for m in relevant if m.get("role") == "resolution"]
    
#     if not resolutions:
#         return "Resolved (no resolution message recorded)."
    
#     # ✅ SHORTER context to avoid 400 error
#     context = f"{incident[:100]} → {'; '.join(resolutions)[:150]}"
    
#     print(f"📝 LLM context: {context[:120]}...")
    
#     url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID
#     }
    
#     # ✅ SHORTER prompt
#     prompt = f"""Summarize: {context}"""
    
#     data = {
#         "prompt": prompt,
#         "model": "crm-di-qwen_text_14b-fp8-it",
#         "system_prompt": "Summarize incident resolution in 1 sentence: what was done.",
#         "temperature": 0.3,
#         "max_tokens": 100,
#     }
    
#     try:
#         resp = requests.post(url, json=data, headers=headers, timeout=30)
#         print(f"🤖 LLM response status: {resp.status_code}")
        
#         if resp.status_code == 200:
#             result = resp.json()
#             output = result.get("data", {}).get("output_text") or result.get("response", "")
#             summary = output.strip()
            
#             if summary and len(summary) > 10:
#                 print(f"✅ LLM summary: {summary[:80]}...")
#                 return summary
#         else:
#             print(f"❌ LLM error: {resp.text[:200]}")
        
#         # Fallback
#         fallback = resolutions[0][:150] if resolutions else "Resolved"
#         print(f"📝 Fallback: {fallback[:80]}...")
#         return fallback
        
#     except Exception as e:
#         print(f"❌ LLM exception: {e}")
#         return resolutions[0][:150] if resolutions else "Resolved"


# def summarize_resolution_with_llm(messages: list) -> str:
#     """Generate resolution summary from linked messages"""
#     if not messages:
#         return "No resolution details available."
    
#     # Get incident + discussion + resolution messages
#     relevant = [m for m in messages if m.get("role") in ["incident", "discussion", "resolution"]]
#     if not relevant:
#         return "Issue resolved (no details recorded)."
    
#     # Build context
#     incident = next((m.get("message_text", "") for m in relevant if m.get("role") == "incident"), "")
#     discussions = [m.get("message_text", "") for m in relevant if m.get("role") == "discussion"][:5]
#     resolutions = [m.get("message_text", "") for m in relevant if m.get("role") == "resolution"]
    
#     if not resolutions:
#         return "Resolved (no resolution message recorded)."
    
#     context = f"Problem: {incident}\n"
#     if discussions:
#         context += f"Investigation: {'; '.join(discussions)}\n"
#     context += f"Resolution: {'; '.join(resolutions)}"
    
#     url = f"https://api.catalyst.zoho.com/quickml/v2/project/{CATALYST_PROJECT_ID}/llm/chat"
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {CATALYST_TOKEN}",
#         "CATALYST-ORG": CATALYST_ORG_ID
#     }
    
#     prompt = f"""Summarize how this incident was resolved in 1-2 sentences:

# {context}

# Summary:"""
    
#     data = {
#         "prompt": prompt,
#         "model": "crm-di-qwen_text_14b-fp8-it",
#         "system_prompt": "Summarize incident resolutions clearly: what was done and the outcome.",
#         "temperature": 0.3,
#         "max_tokens": 150,
#     }
    
#     try:
#         resp = requests.post(url, json=data, headers=headers, timeout=30)
#         if resp.status_code == 200:
#             result = resp.json()
#             output = result.get("data", {}).get("output_text") or result.get("response", "")
#             summary = output.strip()
            
#             if summary and len(summary) > 10:
#                 print(f"✅ LLM summary: {summary[:50]}...")
#                 return summary
        
#         print(f"⚠️ LLM failed, using fallback")
#         # Fallback: just use resolution messages
#         return "; ".join(resolutions)[:200]
        
#     except Exception as e:
#         print(f"❌ LLM error: {e}")
#         return "; ".join(resolutions)[:200] if resolutions else "Resolved"


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
    

    if role == "incident":
        # ✅ Check for duplicate title (including RECENTLY CLOSED ones)
        all_issues = fetch_all_issues()  # Get ALL issues (open + resolved)
        normalized_title = message_text.strip().lower()
        
        # Check last 5 issues (including recently closed)
        for row in all_issues[:5]:
            existing_title = row.get("title", "").strip().lower()
            existing_status = row.get("status", "").lower()
            
            if existing_title == normalized_title:
                # Check if recently closed (within 5 minutes)
                resolved_ts = int(row.get("resolved_at", 0))
                if resolved_ts > 0:
                    time_diff_ms = timestamp_ms - resolved_ts
                    time_diff_minutes = time_diff_ms / (1000 * 60)
                    
                    if time_diff_minutes < 5:
                        print(f"⚠️ Skipping duplicate incident - same title closed {time_diff_minutes:.1f} minutes ago")
                        # Link to the closed issue instead of creating new one
                        issue_id = row.get("issue_id")
                        print(f"🔗 Linking to recently closed issue: {issue_id[:12]}")
                        break
                
                # If open, reuse it
                if existing_status == "open":
                    issue_id = row.get("issue_id")
                    print(f"🔗 Reusing existing open issue: {issue_id}")
                    break
        
        # If not found, try similarity (only for open issues)
        if not issue_id:
            existing_open = fetch_open_issues()
            q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
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
        
        # Create new issue if still no match
        if not issue_id:
            issue_id = str(uuid.uuid4())
            title = message_text[:100] + ("..." if len(message_text) > 100 else "")
            create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
            print(f"🆕 Created new issue: {issue_id}")

    
    # elif role in ["discussion", "resolution"]:
    #     # Try to link to latest open issue
    #     latest_open = get_latest_open_issue_for_conversation(conversation_id)
    #     if latest_open:
    #         issue_id = latest_open.get("issue_id")
    #         print(f"🔗 Linked {role} to latest open issue: {issue_id}")
    #     else:
    #         # Fallback: Get issue_id from previous message
    #         print(f"⚠️ No open issue found, checking previous messages...")
    #         issue_id = get_issue_id_from_last_message(conversation_id)
            
    #         if issue_id:
    #             print(f"🔗 Linked {role} to issue from previous message: {issue_id[:12]}")
    #         else:
    #             print(f"❌ No issue_id found; {role} unlinked.")
        
    #     # IF RESOLUTION: Generate summary and close issue
    #     if role == "resolution" and issue_id:
    #         print(f"🔧 Processing resolution for issue {issue_id[:12]}...")
            
    #         # Fetch existing messages
    #         messages = fetch_messages_by_issue_id(issue_id)
    #         print(f"📥 Found {len(messages)} existing messages for summary")
            
    #         # ✅ Generate summary INCLUDING current resolution message
    #         summary = summarize_resolution_with_llm(messages, current_resolution_text=message_text)
    #         print(f"✅ Generated summary: {summary[:100]}...")
            
    #         # Store summary and close issue
    #         ok = store_resolution_summary(issue_id, summary, timestamp_ms)
    #         if ok:
    #             print(f"✅ Closed issue {issue_id[:12]} with summary")
    #         else:
    #             print(f"❌ Failed to close issue {issue_id[:12]}")

    elif role in ["discussion", "resolution"]:
        # ✅ FOR DISCUSSIONS: Use similarity search to find BEST matching open issue
        latest_open = None
        

        if role == "discussion":
            print(f"💬 Discussion detected, finding best matching issue via similarity...")
            
            # Get all open issues
            open_issues = fetch_open_issues()
            
            if open_issues:
                print(f"🔍 Searching across {len(open_issues)} open issue(s)...")
                
                # Search for similar incidents/discussions in Qdrant
                q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
                hits = qdrant.search(
                    collection_name=QDRANT_COLLECTION,
                    query_vector=emb,
                    query_filter=q_filter,
                    limit=10,
                )
                
                # Filter to only open issues
                open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                
                best_match = None
                best_score = 0.0
                
                for hit in hits:
                    if not hit.payload:
                        continue
                    candidate_id = hit.payload.get("issue_id")
                    if candidate_id and candidate_id in open_issue_ids:
                        if hit.score > best_score:
                            best_match = candidate_id
                            best_score = hit.score
                
                # ✅ LOWER threshold to 0.55, with fallback to previous message
                if best_match and best_score >= 0.55:
                    matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
                    issue_id = best_match
                    print(f"✓ Similarity match found!")
                    print(f"  Issue ID: {issue_id[:12]}")
                    print(f"  Score: {best_score:.3f}")
                    print(f"  Title: {matched_issue.get('title', '')[:60]}")
                else:
                    # ✅ FALLBACK: Get from previous message
                    print(f"⚠️ Low similarity (best: {best_score:.3f}), checking previous message...")
                    issue_id = get_issue_id_from_last_message(conversation_id)
                    
                    if issue_id:
                        print(f"🔗 Linked to issue from previous message: {issue_id[:12]}")
                    else:
                        print(f"❌ No previous message with issue, discussion unlinked")
                        issue_id = None
            else:
                # No open issues - check previous messages
                print(f"⚠️ No open issues, checking previous messages...")
                issue_id = get_issue_id_from_last_message(conversation_id)
                
                if issue_id:
                    print(f"🔗 Linked to issue from previous message: {issue_id[:12]}")
                else:
                    print(f"❌ No issue found, discussion unlinked")
                    issue_id = None

        # if role == "discussion":
        #     print(f"💬 Discussion detected, finding best matching issue via similarity...")
            
        #     # Get all open issues
        #     open_issues = fetch_open_issues()
            
        #     if open_issues:
        #         print(f"🔍 Searching across {len(open_issues)} open issue(s)...")
                
        #         # Search for similar incidents/discussions in Qdrant
        #         q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
        #         hits = qdrant.search(
        #             collection_name=QDRANT_COLLECTION,
        #             query_vector=emb,
        #             query_filter=q_filter,
        #             limit=10,
        #         )
                
        #         # Filter to only open issues
        #         open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                
        #         best_match = None
        #         best_score = 0.0
                
        #         for hit in hits:
        #             if not hit.payload:
        #                 continue
        #             candidate_id = hit.payload.get("issue_id")
        #             if candidate_id and candidate_id in open_issue_ids:
        #                 if hit.score > best_score:
        #                     best_match = candidate_id
        #                     best_score = hit.score
                
        #         # ✅ If good match found (>0.60), link to it
        #         if best_match and best_score >= 0.60:
        #             matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
        #             issue_id = best_match
        #             print(f"✓ Similarity match found!")
        #             print(f"  Issue ID: {issue_id[:12]}")
        #             print(f"  Score: {best_score:.3f}")
        #             print(f"  Title: {matched_issue.get('title', '')[:80]}")
        #         else:
        #             print(f"⚠️ No good match (best score: {best_score:.3f}), discussion unlinked")
        #             issue_id = None
        #     else:
        #         print(f"⚠️ No open issues found, discussion unlinked")
        #         issue_id = None
        
        # elif role == "resolution":
        #     # ✅ RESOLUTION: Also use similarity, but with fallback to previous message
        #     print(f"✅ Resolution detected, finding matching issue...")
            
        #     # Try similarity first
        #     open_issues = fetch_open_issues()
            
        #     if open_issues:
        #         print(f"🔍 Searching across {len(open_issues)} open issue(s)...")
                
        #         q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
        #         hits = qdrant.search(
        #             collection_name=QDRANT_COLLECTION,
        #             query_vector=emb,
        #             query_filter=q_filter,
        #             limit=10,
        #         )
                
        #         open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                
        #         best_match = None
        #         best_score = 0.0
                
        #         for hit in hits:
        #             if not hit.payload:
        #                 continue
        #             candidate_id = hit.payload.get("issue_id")
        #             if candidate_id and candidate_id in open_issue_ids:
        #                 if hit.score > best_score:
        #                     best_match = candidate_id
        #                     best_score = hit.score
                
        #         if best_match and best_score >= 0.60:
        #             matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
        #             issue_id = best_match
        #             print(f"✓ Resolution matched to issue: {issue_id[:12]} (score={best_score:.3f})")
        #         else:
        #             # Fallback: Get from previous message
        #             print(f"⚠️ No similarity match, checking previous messages...")
        #             issue_id = get_issue_id_from_last_message(conversation_id)
                    
        #             if issue_id:
        #                 print(f"🔗 Linked resolution to issue from previous message: {issue_id[:12]}")
        #             else:
        #                 print(f"❌ No issue_id found; resolution unlinked.")
        #     else:
        #         # Fallback: Get from previous message
        #         print(f"⚠️ No open issues, checking previous messages...")
        #         issue_id = get_issue_id_from_last_message(conversation_id)
                
        #         if issue_id:
        #             print(f"🔗 Linked resolution to issue from previous message: {issue_id[:12]}")
        #         else:
        #             print(f"❌ No issue_id found; resolution unlinked.")
        
        # # ✅ IF RESOLUTION: Generate summary and close issue
        # if role == "resolution" and issue_id:
        #     print(f"🔧 Processing resolution for issue {issue_id[:12]}...")
            
        #     # Fetch existing messages
        #     messages = fetch_messages_by_issue_id(issue_id)
        #     print(f"📥 Found {len(messages)} existing messages for summary")
            
        #     # Generate summary INCLUDING current resolution message
        #     summary = summarize_resolution_with_llm(messages, current_resolution_text=message_text)
        #     print(f"✅ Generated summary: {summary[:100]}...")
            
        #     # Store summary and close issue
        #     ok = store_resolution_summary(issue_id, summary, timestamp_ms)
        #     if ok:
        #         print(f"✅ Closed issue {issue_id[:12]} with summary")
        #     else:
        #         print(f"❌ Failed to close issue {issue_id[:12]}")
        elif role == "resolution":
    # ✅ RESOLUTION: Use LLM to check if it's vague or specific
            print(f"✅ Resolution detected: '{message_text[:60]}...'")
            
            specificity = check_resolution_specificity(message_text)
            is_vague = specificity.get("specificity") == "vague"
            
            if is_vague:
                print(f"🎯 Vague resolution (LLM determined) - using previous message's issue")
                
                # Get issue from previous message
                issue_id = get_issue_id_from_last_message(conversation_id)
                
                if issue_id:
                    print(f"🔗 Resolving issue from previous message: {issue_id[:12]}")
                else:
                    print(f"❌ No issue found in previous messages")
            else:
                # Specific resolution - use similarity search
                print(f"🎯 Specific resolution (LLM determined) - using similarity search")
                
                open_issues = fetch_open_issues()
                issue_id = None
                
                if open_issues:
                    print(f"🔍 Searching across {len(open_issues)} open issue(s)...")
                    
                    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
                    hits = qdrant.search(
                        collection_name=QDRANT_COLLECTION,
                        query_vector=emb,
                        query_filter=q_filter,
                        limit=10,
                    )
                    
                    open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                    
                    best_match = None
                    best_score = 0.0
                    
                    for hit in hits:
                        if not hit.payload:
                            continue
                        candidate_id = hit.payload.get("issue_id")
                        if candidate_id and candidate_id in open_issue_ids:
                            if hit.score > best_score:
                                best_match = candidate_id
                                best_score = hit.score
                    
                    if best_match and best_score >= 0.60:
                        matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
                        issue_id = best_match
                        print(f"✓ Similarity match: {issue_id[:12]} (score={best_score:.3f})")
                        print(f"  Matched issue: {matched_issue.get('title', '')[:60]}")
                    else:
                        # Fallback: previous message
                        print(f"⚠️ No similarity match (best: {best_score:.3f}), checking previous messages...")
                        issue_id = get_issue_id_from_last_message(conversation_id)
                        
                        if issue_id:
                            print(f"🔗 Linked to issue from previous message: {issue_id[:12]}")
                        else:
                            print(f"❌ No issue_id found; resolution unlinked")
                else:
                    # No open issues - check previous messages
                    print(f"⚠️ No open issues, checking previous messages...")
                    issue_id = get_issue_id_from_last_message(conversation_id)
                    
                    if issue_id:
                        print(f"🔗 Linked to issue from previous message: {issue_id[:12]}")
                    else:
                        print(f"❌ No issue_id found; resolution unlinked")
            
            # ✅ IF RESOLUTION LINKED TO ISSUE: Generate summary and close
            if issue_id:
                print(f"🔧 Processing resolution for issue {issue_id[:12]}...")
                
                # Fetch existing messages
                messages = fetch_messages_by_issue_id(issue_id)
                print(f"📥 Found {len(messages)} existing messages for summary")
                
                # Generate summary INCLUDING current resolution message
                summary = summarize_resolution_with_llm(messages, current_resolution_text=message_text)
                print(f"✅ Generated summary: {summary[:100]}...")
                
                # Store summary and close issue
                ok = store_resolution_summary(issue_id, summary, timestamp_ms)
                if ok:
                    print(f"✅ Closed issue {issue_id[:12]} with summary")
                else:
                    print(f"❌ Failed to close issue {issue_id[:12]}")


    
    # ✅ 4. Store in Data Store (ALWAYS, for ALL roles)
    row_id = insert_message_into_datastore(
        conversation_id, message_id, sender_id, timestamp_ms,
        message_text, role, category, severity, issue_id
    )
    
    # ✅ 5. Store in Qdrant (ALWAYS, for ALL roles)
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
    print(f"✅ Indexed in Qdrant: {message_id} (role={role})")


# def index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text):
#     """
#     1. Classify with LLM
#     2. Embed message
#     3. Link to issue (or create new issue)
#     4. Store in DS + Qdrant
#     """
#     # 1. LLM Classification
#     cls = classify_message_llm(message_text)
#     role = cls.get("role", "discussion")
#     category = cls.get("category", "other")
#     severity = cls.get("severity", "low")
    
#     print(f"📋 Role: {role}, Category: {category}, Severity: {severity}")
    
#     # 2. Embed
#     emb = embed_text(message_text)
#     ensure_qdrant_collection(len(emb))
    
#     issue_id = None
    
#     # 3. Link to issue
#     if role == "incident":
#         # 3A-1. Reuse existing open issue with identical title (avoid duplicates)
#         existing_open = fetch_open_issues()
#         normalized_title = message_text.strip().lower()
#         for row in existing_open:
#             if row.get("title", "").strip().lower() == normalized_title:
#                 issue_id = row.get("issue_id")
#                 print(f"🔗 Reusing existing open issue for same title: {issue_id}")
#                 break

#         # 3A-2. If not found by title, try similarity to other open incidents
#         if not issue_id:
#             q_filter = Filter(
#                 must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
#             )
#             hits = qdrant.search(
#                 collection_name=QDRANT_COLLECTION,
#                 query_vector=emb,
#                 query_filter=q_filter,
#                 limit=5,
#             )
#             open_ids = {r.get("issue_id") for r in existing_open if r.get("issue_id")}
#             best_issue_id = None
#             best_score = 0.0
#             for h in hits:
#                 if not h.payload:
#                     continue
#                 cand_id = h.payload.get("issue_id")
#                 if not cand_id or cand_id not in open_ids:
#                     continue
#                 if h.score > best_score and h.score >= 0.75:
#                     best_issue_id = cand_id
#                     best_score = h.score
#             if best_issue_id:
#                 issue_id = best_issue_id
#                 print(f"🔗 Linked incident to similar open issue: {issue_id} (score={best_score:.2f})")

#         # 3A-3. If still no issue_id, create a new issue
#         if not issue_id:
#             issue_id = str(uuid.uuid4())
#             title = message_text[:100] + ("..." if len(message_text) > 100 else "")
#             create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
#             print(f"🆕 Created new issue: {issue_id}")
#     # if role == "incident":
#     #     # Check if similar open incident exists
#     #     similar_issue_id, score = find_similar_open_incident(emb, threshold=0.75)
        
#     #     if similar_issue_id:
#     #         print(f"🔗 Linked to existing issue: {similar_issue_id} (score={score:.2f})")
#     #         issue_id = similar_issue_id
#     #     else:
#     #         # Create new issue
#     #         issue_id = str(uuid.uuid4())
#     #         title = message_text[:100] + ("..." if len(message_text) > 100 else "")
#     #         open_issues[issue_id] = {
#     #             "opened_at": timestamp_ms,
#     #             "title": title,
#     #             "category": category,
#     #             "severity": severity,
#     #         }
#     #         create_issue_in_datastore(issue_id, title, "Cliq", category, severity, timestamp_ms)
#     #         print(f"🆕 Created new issue: {issue_id}")
    
#     # elif role in ["discussion", "resolution"]:
#     #     # Link to nearest open issue
#     #     similar_issue_id, score = find_similar_open_incident(emb, threshold=0.65)
#     #     if similar_issue_id:
#     #         issue_id = similar_issue_id
#     #         print(f"🔗 Linked {role} to issue: {issue_id} (score={score:.2f})")
            
#     #         # If resolution, check if we should close the issue
#     #         if role == "resolution":
#     #             # Simple heuristic: if message says "fixed", "resolved", "closed", close it
#     #             if any(w in message_text.lower() for w in ["fixed", "resolved", "closed", "done"]):
#     #                 close_issue_in_datastore(issue_id, timestamp_ms)
#     #                 if issue_id in open_issues:
#     #                     del open_issues[issue_id]
#     #                 print(f"✅ Closed issue: {issue_id}")
    
#     # elif role in ["discussion", "resolution"]:
#     # # Link to nearest open issue using Qdrant, but don't depend on in-memory open_issues
#     #     q_filter = Filter(
#     #         must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
#     #     )
#     #     hits = qdrant.search(
#     #         collection_name=QDRANT_COLLECTION,
#     #         query_vector=emb,
#     #         query_filter=q_filter,
#     #         limit=3,
#     #     )
#     #     linked_issue_id = None
#     #     best_score = 0.0

#     #     for h in hits:
#     #         if not h.payload:
#     #             continue
#     #         candidate_issue_id = h.payload.get("issue_id")
#     #         if not candidate_issue_id:
#     #             continue
#     #         # Check if this issue is still open in Data Store
#     #         open_issues_rows = fetch_open_issues()
#     #         if any(row.get("issue_id") == candidate_issue_id for row in open_issues_rows):
#     #             linked_issue_id = candidate_issue_id
#     #             best_score = h.score
#     #             break

#     #     if linked_issue_id:
#     #         issue_id = linked_issue_id
#     #         print(f"🔗 Linked {role} to issue: {issue_id} (score={best_score:.2f})")

#     #         # If resolution, close the issue
#     #         if role == "resolution" and any(w in message_text.lower() for w in ["fixed", "resolved", "closed", "done"]):
#     #             ok = close_issue_in_datastore(issue_id, timestamp_ms)
#     #             # Also remove from in-memory cache if present
#     #             if ok:
#     #                 if issue_id in open_issues:
#     #                     del open_issues[issue_id]
#     #                 print(f"✅ Closed issue: {issue_id}")
#     #             else:
#     #                 print("failed to close the issue in DS ... DOT")
#     #     else:
#     #         print(f"No open issue found to link this {role} message.")

#     elif role in ["discussion", "resolution"]:
#         issue_id = None

#         # Link to latest open issue (simple and robust)
#         latest_open = get_latest_open_issue_for_conversation(conversation_id)
#         if latest_open:
#             issue_id = latest_open.get("issue_id")
#             print(f"🔗 Linked {role} to latest open issue: {issue_id}")
#         else:
#             print(f"No open issue in DS for conversation {conversation_id}; {role} unlinked.")

#         # If this is a resolution and text clearly says it is fixed/closed, close the issue
#         if role == "resolution" and issue_id and any(
#             w in message_text.lower() for w in ["fixed", "resolved", "closed", "back to normal", "no issues"]
#         ):
#             ok = close_issue_in_datastore(issue_id, timestamp_ms)
#             if ok:
#                 print(f"✅ Closed issue (DS): {issue_id}")
#             else:
#                 print(f"❌ Failed to close issue in DS: {issue_id}")



#         # 4. Store in Data Store
#         row_id = insert_message_into_datastore(
#             conversation_id, message_id, sender_id, timestamp_ms,
#             message_text, role, category, severity, issue_id
#         )
        
#         # 5. Store in Qdrant
#         qdrant_id = normalize_message_id(message_id)
#         point = PointStruct(
#             id=qdrant_id,
#             vector=emb,
#             payload={
#                 "conversation_id": conversation_id,
#                 "sender_id": sender_id,
#                 "role": role,
#                 "category": category,
#                 "severity": severity,
#                 "issue_id": issue_id or "",
#                 "row_id": row_id,
#                 "message_id": message_id,
#             },
#         )
#         qdrant.upsert(QDRANT_COLLECTION, [point])
#         print(f"✅ Indexed in Qdrant: {message_id}")


# ---------- Command Handlers ----------

def handle_search_command(query: str, chat: dict, user: dict):
    """
    Search for past issues (not individual messages).
    Show incident + resolution summary + metadata.
    """
    print(f"Searching issues for: {query!r}")
    q_emb = embed_text(query)
    
    # Search incidents only
    q_filter = Filter(
        must=[FieldCondition(key="role", match=MatchValue(value="incident"))]
    )
    
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=5,
    )
    
    if not hits:
        return jsonify({"text": f"No similar incidents found for `{query}`."})
    
    # Group by issue_id
    issue_ids = list({h.payload.get("issue_id") for h in hits if h.payload and h.payload.get("issue_id")})
    
    if not issue_ids:
        return jsonify({"text": "No past issues found."})
    
    lines = [f"*Past incidents similar to:* `{query}`\n"]
    
    for idx, issue_id in enumerate(issue_ids[:3], 1):
        # Fetch all messages for this issue
        messages = fetch_messages_by_issue_id(issue_id)
        if not messages:
            continue
        
        # Find incident message
        incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
        if not incident_msg:
            continue
        
        title = incident_msg.get("message_text", "")[:100]
        category = incident_msg.get("category", "other")
        severity = incident_msg.get("severity", "low")
        
        # Check if resolved
        resolution_msgs = [m for m in messages if m.get("role") == "resolution"]
        if resolution_msgs:
            resolution_summary = summarize_resolution_with_llm(messages)
            resolved_at_ts = max([m.get("time_stamp", 0) for m in resolution_msgs])
            resolved_at = datetime.fromtimestamp(resolved_at_ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
            status = "Resolved"
        else:
            resolution_summary = "Open (no resolution yet)"
            resolved_at = "—"
            status = "Open"
        
        lines.append(f"*#{idx}. {title}*")
        lines.append(f"Category: {category} | Severity: {severity} | Status: {status}")
        lines.append(f"Resolution: {resolution_summary}")
        if resolved_at != "—":
            lines.append(f"Resolved at: {resolved_at}")
        lines.append("")  # blank line
    
    return jsonify({"text": "\n".join(lines)})


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
    
    # Get message details
    message_obj = raw.get("message", {})
    message_id = message_obj.get("id")
    timestamp_ms = raw.get("time")
    sender_id = user.get("zoho_user_id") or user.get("id")
    conversation_id = chat.get("id")
    
    # ✅ CHECK FOR ATTACHMENTS
    attachments = message_obj.get("attachments", [])
    if attachments:
        print(f"📎 Found {len(attachments)} attachment(s)")
        for att in attachments:
            process_attachment(att, conversation_id, sender_id, timestamp_ms)
        return jsonify({"status": "processed_attachments"})
    
    # Regular text message processing
    try:
        message_text = message_obj.get("content", {}).get("text", "")
    except:
        return jsonify({"status": "bad_raw"}), 200
    
    if not message_text:
        return jsonify({"status": "no_text"}), 200
    
    # Skip bot commands
    txt_lower = message_text.lower()
    if "@workspace-vita" in txt_lower or "{@b-" in message_text:
        print(f"Skipping bot command from indexing: {message_text[:50]}")
        return jsonify({"status": "command_skipped"}), 200
    
    # Check if already indexed
    qdrant_id = normalize_message_id(message_id)
    try:
        existing = qdrant.retrieve(
            collection_name=QDRANT_COLLECTION,
            ids=[qdrant_id]
        )
        if existing:
            print(f"⚠️ Message {message_id} already indexed, skipping")
            return jsonify({"status": "already_indexed"}), 200
    except Exception:
        pass
    
    print(f"📨 '{message_text}'")
    
    index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)
    
    return jsonify({"status": "processed"})

@app.route('/process_attachment_cliq', methods=['POST'])
def process_attachment_cliq():
    """Process attachment from Cliq Participation Handler (URL-based)"""
    try:
        print("\n" + "="*60)
        print("📎 CLIQ ATTACHMENT RECEIVED")
        print("="*60)
        
        # Get parameters
        file_url = request.form.get('file_url')
        file_name = request.form.get('file_name', f'attachment_{uuid.uuid4()}')
        conversation_id = request.form.get('conversation_id')
        sender_id = request.form.get('sender_id')
        timestamp_ms = int(request.form.get('timestamp', 0))
        message_id = request.form.get('message_id')
        
        print(f"File: {file_name}")
        print(f"URL: {file_url[:80]}...")
        print(f"Conversation: {conversation_id}")
        
        if not file_url:
            return jsonify({"error": "No file URL"}), 400
        
        # Download file from Cliq
        ext = file_name.split(".")[-1].lower() if "." in file_name else "bin"
        temp_path = tempfile.mktemp(suffix=f".{ext}")
        
        print(f"⬇️ Downloading from Cliq...")
        resp = requests.get(file_url, stream=True, timeout=60)
        
        with open(temp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024*1024):
                if chunk:
                    f.write(chunk)
        
        file_size = os.path.getsize(temp_path)
        print(f"✓ Downloaded {file_size} bytes")
        
        try:
            # Upload to Stratus
            stratus_key = f"attachments/{message_id}_{file_name}"
            stratus_url = upload_to_stratus(temp_path, stratus_key)
            
            if not stratus_url:
                return jsonify({"error": "Stratus upload failed"}), 500
            
            print(f"✓ Uploaded to Stratus: {stratus_url}")
            
            # ✅ GET LATEST OPEN ISSUE FOR LINKING
            latest_issue = get_latest_open_issue_for_conversation(conversation_id)
            issue_id = latest_issue.get("issue_id") if latest_issue else None
            
            if issue_id:
                print(f"🔗 Will link to open issue: {issue_id[:12]}")
            else:
                print(f"⚠️ No open issue to link to")
            
            # Process based on file type

            if ext in ("png", "jpg", "jpeg", "bmp", "webp", "gif"):
                print("🖼️ Processing image with Vision model...")
                analysis = process_image_with_vision(temp_path)
                
                if not analysis:
                    analysis = "Image uploaded (vision analysis unavailable)"
                    print("⚠️ Vision model returned empty, treating as discussion")
                    
                    # Get latest issue for linking discussions
                    latest_issue = get_latest_open_issue_for_conversation(conversation_id)
                    issue_id = latest_issue.get("issue_id") if latest_issue else None
                    
                    # Store as discussion
                    message_text = f"User shared image: {stratus_url}\n\nVision analysis unavailable."
                    
                    insert_message_into_datastore(
                        conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
                        message_text, "discussion", "other", "low", issue_id
                    )
                    
                    # Index in Qdrant
                    emb = embed_text(message_text)
                    ensure_qdrant_collection(len(emb))
                    
                    qdrant_id = normalize_message_id(f"img_{message_id}")
                    point = PointStruct(
                        id=qdrant_id,
                        vector=emb,
                        payload={
                            "conversation_id": conversation_id,
                            "sender_id": sender_id,
                            "role": "discussion",
                            "category": "other",
                            "severity": "low",
                            "issue_id": issue_id or "",
                            "message_id": f"img_{message_id}",
                        },
                    )
                    qdrant.upsert(QDRANT_COLLECTION, [point])
                    
                    return jsonify({
                        "status": "discussion_created",
                        "analysis": "Vision analysis unavailable",
                        "linked_issue": issue_id[:12] if issue_id else None
                    })
                
                print(f"Vision analysis: {analysis[:150]}...")
                
                # ✅ USE LLM TO CLASSIFY THE VISION ANALYSIS OUTPUT
                print("🤖 Classifying vision analysis with LLM...")
                classification = classify_message_llm(analysis)
                
                role = classification.get("role", "discussion")
                category = classification.get("category", "other")
                severity = classification.get("severity", "low")
                
                print(f"✅ LLM Classification: role={role}, category={category}, severity={severity}")
                
                # ✅ IF INCIDENT: Check similarity with ALL open issues FIRST
                
                # ✅ IF INCIDENT: Check for RECENT text incident first, then similarity
                if role == "incident":
                    print("🚨 LLM DETECTED INCIDENT IN IMAGE!")
                    
                    # Embed the vision analysis
                    incident_emb = embed_text(analysis)
                    ensure_qdrant_collection(len(incident_emb))
                    
                    # ✅ STEP 1: Get ALL open issues
                    open_issues = fetch_open_issues()
                    
                    matched_issue_id = None
                    
                    if open_issues:
                        print(f"🔍 Checking for recent incidents and similarity with {len(open_issues)} open issue(s)...")
                        
                        # ✅ PRIORITY 1: Check for RECENT incident (within last 2 minutes) with same category
                        recent_threshold_ms = 2 * 60 * 1000  # 2 minutes
                        current_time_ms = timestamp_ms
                        
                        recent_matches = []
                        for issue in open_issues:
                            opened_at = int(issue.get("opened_at", 0))
                            time_diff = current_time_ms - opened_at
                            
                            if time_diff <= recent_threshold_ms and time_diff >= 0:
                                issue_category = issue.get("category", "").lower()
                                issue_severity = issue.get("severity", "").lower()
                                
                                # Same category or both high severity
                                if issue_category == category.lower() or (issue_severity == "high" and severity.lower() == "high"):
                                    recent_matches.append({
                                        "issue": issue,
                                        "time_diff": time_diff
                                    })
                        
                        if recent_matches:
                            # Sort by most recent
                            recent_matches.sort(key=lambda x: x["time_diff"])
                            recent_issue = recent_matches[0]["issue"]
                            matched_issue_id = recent_issue.get("issue_id")
                            
                            print(f"🎯 Found RECENT incident ({recent_matches[0]['time_diff']/1000:.0f}s ago)!")
                            print(f"  Issue ID: {matched_issue_id[:12]}")
                            print(f"  Title: {recent_issue.get('title', '')[:80]}")
                            print(f"  Category: {recent_issue.get('category')} (matches: {category})")
                        
                        # ✅ PRIORITY 2: If no recent match, use similarity search
                        if not matched_issue_id:
                            print("🔍 No recent match, using similarity search...")
                            
                            q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
                            hits = qdrant.search(
                                collection_name=QDRANT_COLLECTION,
                                query_vector=incident_emb,
                                query_filter=q_filter,
                                limit=10,
                            )
                            
                            # Get set of open issue IDs
                            open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                            
                            # Find best match among open issues
                            best_match = None
                            best_score = 0.0
                            
                            for hit in hits:
                                if not hit.payload:
                                    continue
                                candidate_id = hit.payload.get("issue_id")
                                if candidate_id and candidate_id in open_issue_ids:
                                    if hit.score > best_score:
                                        best_match = candidate_id
                                        best_score = hit.score
                            
                            # ✅ RAISE threshold to 0.85 for better matching
                            if best_match and best_score >= 0.85:
                                matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
                                print(f"✓ High similarity match found!")
                                print(f"  Issue ID: {best_match[:12]}")
                                print(f"  Score: {best_score:.3f}")
                                print(f"  Title: {matched_issue.get('title', '')[:80]}")
                                
                                matched_issue_id = best_match
                            else:
                                print(f"⚠️ No high similarity match (best: {best_score:.3f}, threshold: 0.85)")
                    
                    # ✅ STEP 2: If matched, link as discussion to that issue
                    if matched_issue_id:
                        message_text = f"User shared image showing related incident: {stratus_url}\n\nVision analysis: {analysis}"
                        
                        insert_message_into_datastore(
                            conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
                            message_text, "discussion", category, severity, matched_issue_id
                        )
                        
                        # Index in Qdrant
                        qdrant_id = normalize_message_id(f"img_{message_id}")
                        point = PointStruct(
                            id=qdrant_id,
                            vector=incident_emb,
                            payload={
                                "conversation_id": conversation_id,
                                "sender_id": sender_id,
                                "role": "discussion",
                                "category": category,
                                "severity": severity,
                                "issue_id": matched_issue_id,
                                "message_id": f"img_{message_id}",
                            },
                        )
                        qdrant.upsert(QDRANT_COLLECTION, [point])
                        
                        print(f"✅ Linked image to existing issue: {matched_issue_id[:12]}")
                        
                        return jsonify({
                            "status": "linked_to_existing_issue",
                            "issue_id": matched_issue_id[:12],
                            "method": "recent_match" if recent_matches else "similarity",
                            "analysis": analysis[:200],
                            "stratus_url": stratus_url
                        })
                    
                    # ✅ STEP 3: No match found - create NEW incident
                    else:
                        print("🆕 No similar issue found, creating new incident")
                        
                        # Extract concise title from vision analysis
                        incident_title = extract_incident_title_from_analysis(analysis)
                        
                        message_text = f"{incident_title}\n\n[Image Analysis Details]\n{analysis}\n\nImage: {stratus_url}"
                        
                        # Create new incident using index_message
                        index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                        
                        return jsonify({
                            "status": "incident_created",
                            "title": incident_title,
                            "analysis": analysis[:200],
                            "category": category,
                            "severity": severity,
                            "stratus_url": stratus_url
                        })

                
                
                # if role == "incident":
                #     print("🚨 LLM DETECTED INCIDENT IN IMAGE!")
                    
                #     # Embed the vision analysis
                #     incident_emb = embed_text(analysis)
                #     ensure_qdrant_collection(len(incident_emb))
                    
                #     # ✅ STEP 1: Get ALL open issues
                #     open_issues = fetch_open_issues()
                    
                #     matched_issue_id = None
                    
                #     if open_issues:
                #         print(f"🔍 Checking similarity with {len(open_issues)} open issue(s)...")
                        
                #         # ✅ STEP 2: Search for similar incidents in Qdrant
                #         q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
                #         hits = qdrant.search(
                #             collection_name=QDRANT_COLLECTION,
                #             query_vector=incident_emb,
                #             query_filter=q_filter,
                #             limit=10,
                #         )
                        
                #         # Get set of open issue IDs
                #         open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                        
                #         # Find best match among open issues
                #         best_match = None
                #         best_score = 0.0
                        
                #         for hit in hits:
                #             if not hit.payload:
                #                 continue
                #             candidate_id = hit.payload.get("issue_id")
                #             if candidate_id and candidate_id in open_issue_ids:
                #                 if hit.score > best_score:
                #                     best_match = candidate_id
                #                     best_score = hit.score
                        
                #         # ✅ STEP 3: If similarity >= 0.70, link to that issue
                #         if best_match and best_score >= 0.80:
                #             matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
                #             print(f"✓ High similarity match found!")
                #             print(f"  Issue ID: {best_match[:12]}")
                #             print(f"  Score: {best_score:.3f}")
                #             print(f"  Title: {matched_issue.get('title', '')[:80]}")
                            
                #             matched_issue_id = best_match
                    
                #     # ✅ STEP 4A: If matched, link as discussion to that issue
                #     if matched_issue_id:
                #         message_text = f"User shared image showing related incident: {stratus_url}\n\nVision analysis: {analysis}"
                        
                #         insert_message_into_datastore(
                #             conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
                #             message_text, "discussion", category, severity, matched_issue_id
                #         )
                        
                #         # Index in Qdrant
                #         qdrant_id = normalize_message_id(f"img_{message_id}")
                #         point = PointStruct(
                #             id=qdrant_id,
                #             vector=incident_emb,
                #             payload={
                #                 "conversation_id": conversation_id,
                #                 "sender_id": sender_id,
                #                 "role": "discussion",
                #                 "category": category,
                #                 "severity": severity,
                #                 "issue_id": matched_issue_id,
                #                 "message_id": f"img_{message_id}",
                #             },
                #         )
                #         qdrant.upsert(QDRANT_COLLECTION, [point])
                        
                #         print(f"✅ Linked image to existing issue: {matched_issue_id[:12]}")
                        
                #         return jsonify({
                #             "status": "linked_to_existing_issue",
                #             "issue_id": matched_issue_id[:12],
                #             "similarity_score": round(best_score, 3) if 'best_score' in locals() else None,
                #             "analysis": analysis[:200],
                #             "stratus_url": stratus_url
                #         })
                    
                #     # ✅ STEP 4B: No match found - create NEW incident
                #     # else:
                #     #     print("🆕 No similar issue found (threshold=0.70), creating new incident")
                #     #     message_text = f"[Image Analysis - Incident Detected]\n\n{analysis}\n\nImage: {stratus_url}"
                        
                #     #     # Create new incident using index_message
                #     #     index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                        
                #     #     return jsonify({
                #     #         "status": "incident_created",
                #     #         "analysis": analysis[:200],
                #     #         "category": category,
                #     #         "severity": severity,
                #     #         "stratus_url": stratus_url
                #     #     })

                #     # ✅ STEP 4B: No match found - create NEW incident
                #     else:
                #         print("🆕 No similar issue found (threshold=0.80), creating new incident")
                        
                #         # ✅ Extract concise title from vision analysis
                #         incident_title = extract_incident_title_from_analysis(analysis)
                        
                #         message_text = f"{incident_title}\n\n[Image Analysis Details]\n{analysis}\n\nImage: {stratus_url}"
                        
                #         # Create new incident using index_message
                #         index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                        
                #         return jsonify({
                #             "status": "incident_created",
                #             "title": incident_title,
                #             "analysis": analysis[:200],
                #             "category": category,
                #             "severity": severity,
                #             "stratus_url": stratus_url
                #         })

                
                else:
                    # ✅ DISCUSSION - Link to latest open issue
                    latest_issue = get_latest_open_issue_for_conversation(conversation_id)
                    issue_id = latest_issue.get("issue_id") if latest_issue else None
                    
                    print(f"💬 Image classified as {role}")
                    if issue_id:
                        print(f"🔗 Linking to latest open issue: {issue_id[:12]}")
                    
                    message_text = f"User shared image: {stratus_url}\n\nVision analysis: {analysis}"
                    
                    # Store in DataStore
                    insert_message_into_datastore(
                        conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
                        message_text, role, category, severity, issue_id
                    )
                    
                    # Index in Qdrant
                    emb = embed_text(message_text)
                    ensure_qdrant_collection(len(emb))
                    
                    qdrant_id = normalize_message_id(f"img_{message_id}")
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
                            "message_id": f"img_{message_id}",
                        },
                    )
                    qdrant.upsert(QDRANT_COLLECTION, [point])
                    print(f"✅ Indexed image {role} in Qdrant")
                    
                    return jsonify({
                        "status": f"{role}_created",
                        "analysis": analysis[:200],
                        "category": category,
                        "severity": severity,
                        "linked_issue": issue_id[:12] if issue_id else None
                    })

            # if ext in ("png", "jpg", "jpeg", "bmp", "webp", "gif"):
            #     print("🖼️ Processing image with Vision model...")
            #     analysis = process_image_with_vision(temp_path)
                
            #     if not analysis:
            #         analysis = "Image uploaded (vision analysis unavailable)"
            #         print("⚠️ Vision model returned empty, treating as discussion")
                    
            #         # Store as discussion
            #         message_text = f"User shared image: {stratus_url}\n\nVision analysis unavailable."
                    
            #         insert_message_into_datastore(
            #             conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
            #             message_text, "discussion", "other", "low", issue_id
            #         )
                    
            #         # Index in Qdrant
            #         emb = embed_text(message_text)
            #         ensure_qdrant_collection(len(emb))
                    
            #         qdrant_id = normalize_message_id(f"img_{message_id}")
            #         point = PointStruct(
            #             id=qdrant_id,
            #             vector=emb,
            #             payload={
            #                 "conversation_id": conversation_id,
            #                 "sender_id": sender_id,
            #                 "role": "discussion",
            #                 "category": "other",
            #                 "severity": "low",
            #                 "issue_id": issue_id or "",
            #                 "message_id": f"img_{message_id}",
            #             },
            #         )
            #         qdrant.upsert(QDRANT_COLLECTION, [point])
                    
            #         return jsonify({
            #             "status": "discussion_created",
            #             "analysis": "Vision analysis unavailable",
            #             "linked_issue": issue_id[:12] if issue_id else None
            #         })
                
            #     print(f"Vision analysis: {analysis[:150]}...")
                
            #     # ✅ USE LLM TO CLASSIFY THE VISION ANALYSIS OUTPUT
            #     print("🤖 Classifying vision analysis with LLM...")
            #     classification = classify_message_llm(analysis)
                
            #     role = classification.get("role", "discussion")
            #     category = classification.get("category", "other")
            #     severity = classification.get("severity", "low")
                
            #     print(f"✅ LLM Classification: role={role}, category={category}, severity={severity}")
                
            #     # ✅ CREATE INCIDENT ONLY IF LLM CLASSIFIES AS INCIDENT
            #     # if role == "incident":
            #     #     print("🚨 LLM DETECTED INCIDENT IN IMAGE!")
            #     #     message_text = f"[Image Analysis - Incident Detected]\n\n{analysis}\n\nImage: {stratus_url}"
                    
            #     #     # Use index_message to create incident (handles issue creation)
            #     #     index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                    
            #     #     return jsonify({
            #     #         "status": "incident_created",
            #     #         "analysis": analysis,
            #     #         "category": category,
            #     #         "severity": severity,
            #     #         "stratus_url": stratus_url
            #     #     })
            #     # ✅ CREATE INCIDENT ONLY IF LLM CLASSIFIES AS INCIDENT
            #     if role == "incident":
            #         print("🚨 LLM DETECTED INCIDENT IN IMAGE!")
            #         message_text = f"[Image Analysis - Incident Detected]\n\n{analysis}\n\nImage: {stratus_url}"
                    
            #         # ✅ CHECK IF THIS INCIDENT IS SIMILAR TO ANY EXISTING OPEN ISSUE
            #         print("🔍 Checking similarity with existing open issues...")
                    
            #         # Embed the vision analysis
            #         incident_emb = embed_text(analysis)
            #         ensure_qdrant_collection(len(incident_emb))
                    
            #         # Get all open issues
            #         open_issues = fetch_open_issues()
                    
            #         if open_issues:
            #             # Search for similar incidents in Qdrant
            #             q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
            #             hits = qdrant.search(
            #                 collection_name=QDRANT_COLLECTION,
            #                 query_vector=incident_emb,
            #                 query_filter=q_filter,
            #                 limit=10,
            #             )
                        
            #             # Filter hits to only include open issues
            #             open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                        
            #             best_match = None
            #             best_score = 0.0
                        
            #             for hit in hits:
            #                 if not hit.payload:
            #                     continue
            #                 candidate_id = hit.payload.get("issue_id")
            #                 if candidate_id and candidate_id in open_issue_ids:
            #                     if hit.score > best_score:
            #                         best_match = candidate_id
            #                         best_score = hit.score
                        
            #             # ✅ If high similarity found (>0.70), link to existing issue instead of creating new one
            #             if best_match and best_score >= 0.70:
            #                 matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
            #                 print(f"🔗 High similarity found! Linking to existing issue: {best_match[:12]} (score={best_score:.3f})")
            #                 print(f"   Matched issue: {matched_issue.get('title', '')[:60]}")
                            
            #                 # Store as discussion linked to the matched issue
            #                 message_text_linked = f"User shared image showing related incident: {stratus_url}\n\nVision analysis: {analysis}"
                            
            #                 insert_message_into_datastore(
            #                     conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
            #                     message_text_linked, "discussion", category, severity, best_match
            #                 )
                            
            #                 # Index in Qdrant
            #                 qdrant_id = normalize_message_id(f"img_{message_id}")
            #                 point = PointStruct(
            #                     id=qdrant_id,
            #                     vector=incident_emb,
            #                     payload={
            #                         "conversation_id": conversation_id,
            #                         "sender_id": sender_id,
            #                         "role": "discussion",
            #                         "category": category,
            #                         "severity": severity,
            #                         "issue_id": best_match,
            #                         "message_id": f"img_{message_id}",
            #                     },
            #                 )
            #                 qdrant.upsert(QDRANT_COLLECTION, [point])
                            
            #                 return jsonify({
            #                     "status": "linked_to_existing_issue",
            #                     "issue_id": best_match[:12],
            #                     "similarity_score": round(best_score, 3),
            #                     "analysis": analysis,
            #                     "stratus_url": stratus_url
            #                 })
                    
            #         # ✅ No similar issue found - create new incident
            #         print("🆕 No similar issue found, creating new incident")
            #         index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                    
            #         return jsonify({
            #             "status": "incident_created",
            #             "analysis": analysis,
            #             "category": category,
            #             "severity": severity,
            #             "stratus_url": stratus_url
            #         })

            #     else:
            #         # ✅ DISCUSSION - Link to open issue if exists
            #         print(f"💬 Image stored as {role}")
            #         message_text = f"User shared image: {stratus_url}\n\nVision analysis: {analysis}"
                    
            #         # Store in DataStore (linked to issue if exists)
            #         insert_message_into_datastore(
            #             conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
            #             message_text, role, category, severity, issue_id
            #         )
                    
            #         # ✅ Index in Qdrant
            #         emb = embed_text(message_text)
            #         ensure_qdrant_collection(len(emb))
                    
            #         qdrant_id = normalize_message_id(f"img_{message_id}")
            #         point = PointStruct(
            #             id=qdrant_id,
            #             vector=emb,
            #             payload={
            #                 "conversation_id": conversation_id,
            #                 "sender_id": sender_id,
            #                 "role": role,
            #                 "category": category,
            #                 "severity": severity,
            #                 "issue_id": issue_id or "",
            #                 "message_id": f"img_{message_id}",
            #             },
            #         )
            #         qdrant.upsert(QDRANT_COLLECTION, [point])
            #         print(f"✅ Indexed image {role} in Qdrant")
                    
            #         return jsonify({
            #             "status": f"{role}_created",
            #             "analysis": analysis,
            #             "category": category,
            #             "severity": severity,
            #             "linked_issue": issue_id[:12] if issue_id else None
            #         })
            
            elif ext in ("pdf", "doc", "docx", "txt"):
                print("📄 Processing document with OCR...")
                ocr_text = ocr_document(temp_path, file_name)
                
                if ocr_text:
                    print(f"✓ OCR extracted {len(ocr_text)} characters")
                    print(f"📝 First 100 chars: {ocr_text[:100]}")
                    summary = f"Document '{file_name}' contains: {ocr_text[:300]}..."
                else:
                    print("⚠️ OCR returned empty, using fallback")
                    summary = f"Document '{file_name}' uploaded (text extraction unavailable)"
                
                # ✅ USE SIMILARITY TO FIND BEST MATCHING ISSUE (not latest)
                print("🔍 Finding best matching issue for document via similarity...")
                
                # Embed the document content
                doc_emb = embed_text(summary + " " + ocr_text[:500] if ocr_text else summary)
                ensure_qdrant_collection(len(doc_emb))
                
                # Get all open issues
                open_issues = fetch_open_issues()
                matched_issue_id = None
                
                if open_issues:
                    print(f"🔍 Checking similarity with {len(open_issues)} open issue(s)...")
                    
                    # Search for similar incidents
                    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
                    hits = qdrant.search(
                        collection_name=QDRANT_COLLECTION,
                        query_vector=doc_emb,
                        query_filter=q_filter,
                        limit=10,
                    )
                    
                    # Filter to only open issues
                    open_issue_ids = {issue.get("issue_id") for issue in open_issues}
                    
                    best_match = None
                    best_score = 0.0
                    
                    for hit in hits:
                        if not hit.payload:
                            continue
                        candidate_id = hit.payload.get("issue_id")
                        if candidate_id and candidate_id in open_issue_ids:
                            if hit.score > best_score:
                                best_match = candidate_id
                                best_score = hit.score
                    
                    # ✅ If good match (>=0.65), link to it
                    if best_match and best_score >= 0.65:
                        matched_issue = next((i for i in open_issues if i.get("issue_id") == best_match), None)
                        matched_issue_id = best_match
                        print(f"✓ Document matched to issue!")
                        print(f"  Issue ID: {matched_issue_id[:12]}")
                        print(f"  Score: {best_score:.3f}")
                        print(f"  Title: {matched_issue.get('title', '')[:80]}")
                    else:
                        print(f"⚠️ No good match (best: {best_score:.3f}), document unlinked")
                else:
                    print("⚠️ No open issues, document unlinked")
                
                # Store document discussion
                message_text = f"User shared document: {stratus_url}\n\n{summary}"
                
                insert_message_into_datastore(
                    conversation_id, f"doc_{message_id}", sender_id, timestamp_ms,
                    message_text, "discussion", "other", "low", matched_issue_id
                )
                
                # Index in Qdrant
                qdrant_id = normalize_message_id(f"doc_{message_id}")
                point = PointStruct(
                    id=qdrant_id,
                    vector=doc_emb,
                    payload={
                        "conversation_id": conversation_id,
                        "sender_id": sender_id,
                        "role": "discussion",
                        "category": "other",
                        "severity": "low",
                        "issue_id": matched_issue_id or "",
                        "message_id": f"doc_{message_id}",
                    },
                )
                qdrant.upsert(QDRANT_COLLECTION, [point])
                print(f"✅ Indexed document discussion in Qdrant")
                
                return jsonify({
                    "status": "discussion_created", 
                    "summary": summary[:200],
                    "linked_issue": matched_issue_id[:12] if matched_issue_id else None,
                    "similarity_score": round(best_score, 3) if 'best_score' in locals() and matched_issue_id else None
                })

            # elif ext in ("pdf", "doc", "docx", "txt"):
            #     print("📄 Processing document with OCR...")
            #     ocr_text = ocr_document(temp_path, file_name)
                
            #     if ocr_text:
            #         print(f"✓ OCR extracted {len(ocr_text)} characters")
            #         summary = f"Document '{file_name}' contains: {ocr_text[:300]}..."
            #     else:
            #         print("⚠️ OCR returned empty, using fallback")
            #         summary = f"Document '{file_name}' uploaded (text extraction unavailable)"
                
            #     # ✅ ALWAYS DISCUSSION - Link to open issue if exists
            #     message_text = f"User shared document: {stratus_url}\n\n{summary}"
                
            #     # Store in DataStore (linked to issue)
            #     insert_message_into_datastore(
            #         conversation_id, f"doc_{message_id}", sender_id, timestamp_ms,
            #         message_text, "discussion", "other", "low", issue_id
            #     )
                
            #     # ✅ Index in Qdrant
            #     emb = embed_text(message_text)
            #     ensure_qdrant_collection(len(emb))
                
            #     qdrant_id = normalize_message_id(f"doc_{message_id}")
            #     point = PointStruct(
            #         id=qdrant_id,
            #         vector=emb,
            #         payload={
            #             "conversation_id": conversation_id,
            #             "sender_id": sender_id,
            #             "role": "discussion",
            #             "category": "other",
            #             "severity": "low",
            #             "issue_id": issue_id or "",
            #             "message_id": f"doc_{message_id}",
            #         },
            #     )
            #     qdrant.upsert(QDRANT_COLLECTION, [point])
            #     print(f"✅ Indexed document discussion in Qdrant")
                
            #     return jsonify({
            #         "status": "discussion_created", 
            #         "summary": summary,
            #         "linked_issue": issue_id[:12] if issue_id else None
            #     })
            
            else:
                # ✅ OTHER FILES - Link to issue
                message_text = f"User shared file: {file_name} ({stratus_url})"
                
                insert_message_into_datastore(
                    conversation_id, f"file_{message_id}", sender_id, timestamp_ms,
                    message_text, "discussion", "other", "low", issue_id
                )
                
                # Index in Qdrant
                emb = embed_text(message_text)
                ensure_qdrant_collection(len(emb))
                
                qdrant_id = normalize_message_id(f"file_{message_id}")
                point = PointStruct(
                    id=qdrant_id,
                    vector=emb,
                    payload={
                        "conversation_id": conversation_id,
                        "sender_id": sender_id,
                        "role": "discussion",
                        "category": "other",
                        "severity": "low",
                        "issue_id": issue_id or "",
                        "message_id": f"file_{message_id}",
                    },
                )
                qdrant.upsert(QDRANT_COLLECTION, [point])
                
                return jsonify({
                    "status": "discussion_created",
                    "linked_issue": issue_id[:12] if issue_id else None
                })
        
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print("🗑️ Cleaned up temp file")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500




# @app.route('/process_attachment_cliq', methods=['POST'])
# def process_attachment_cliq():
#     """Process attachment from Cliq Participation Handler (URL-based)"""
#     try:
#         print("\n" + "="*60)
#         print("📎 CLIQ ATTACHMENT RECEIVED")
#         print("="*60)
        
#         # Get parameters
#         file_url = request.form.get('file_url')
#         file_name = request.form.get('file_name', f'attachment_{uuid.uuid4()}')
#         conversation_id = request.form.get('conversation_id')
#         sender_id = request.form.get('sender_id')
#         timestamp_ms = int(request.form.get('timestamp', 0))
#         message_id = request.form.get('message_id')
        
#         print(f"File: {file_name}")
#         print(f"URL: {file_url[:80]}...")
#         print(f"Conversation: {conversation_id}")
        
#         if not file_url:
#             return jsonify({"error": "No file URL"}), 400
        
#         # Download file from Cliq
#         ext = file_name.split(".")[-1].lower() if "." in file_name else "bin"
#         temp_path = tempfile.mktemp(suffix=f".{ext}")
        
#         print(f"⬇️ Downloading from Cliq...")
#         resp = requests.get(file_url, stream=True, timeout=60)
        
#         with open(temp_path, "wb") as f:
#             for chunk in resp.iter_content(chunk_size=1024*1024):
#                 if chunk:
#                     f.write(chunk)
        
#         file_size = os.path.getsize(temp_path)
#         print(f"✓ Downloaded {file_size} bytes")
        
#         try:
#             # Upload to Stratus
#             stratus_key = f"attachments/{message_id}_{file_name}"
#             stratus_url = upload_to_stratus(temp_path, stratus_key)
            
#             if not stratus_url:
#                 return jsonify({"error": "Stratus upload failed"}), 500
            
#             print(f"✓ Uploaded to Stratus: {stratus_url}")
            
#             # Process based on file type
#             if ext in ("png", "jpg", "jpeg", "bmp", "webp", "gif"):
#                 print("🖼️ Processing image with Vision model...")
#                 analysis = process_image_with_vision(temp_path)
                
#                 if not analysis:
#                     analysis = "Image uploaded (vision analysis unavailable)"
                
#                 print(f"Vision: {analysis[:100]}...")
                
#                 # Check for incident
#                 analysis_lower = analysis.lower()
#                 incident_keywords = ["error", "failed", "down", "outage", "critical", 
#                                     "production", "timeout", "crash", "exception", "database"]
                
#                 is_incident = any(kw in analysis_lower for kw in incident_keywords)
                
#                 if is_incident and "no incident" not in analysis_lower:
#                     print("🚨 INCIDENT DETECTED IN IMAGE!")
#                     message_text = f"[Image Analysis] {analysis}"
#                     index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                    
#                     return jsonify({
#                         "status": "incident_created",
#                         "analysis": analysis,
#                         "stratus_url": stratus_url
#                     })
#                 else:
#                     print("💬 Image stored as discussion")
#                     message_text = f"User shared image: {stratus_url}\n\nVision analysis: {analysis}"
#                     insert_message_into_datastore(
#                         conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
#                         message_text, "discussion", "other", "low", None
#                     )
                    
#                     return jsonify({
#                         "status": "discussion_created",
#                         "analysis": analysis
#                     })
            
#             elif ext in ("pdf", "doc", "docx", "txt"):
#                 print("📄 Processing document with OCR...")
#                 ocr_text = ocr_document(temp_path, file_name)
                
#                 if ocr_text:
#                     summary = f"Document '{file_name}' contains: {ocr_text[:200]}..."
#                 else:
#                     summary = f"Document '{file_name}' uploaded"
                
#                 message_text = f"User shared document: {stratus_url}\n\n{summary}"
#                 insert_message_into_datastore(
#                     conversation_id, f"doc_{message_id}", sender_id, timestamp_ms,
#                     message_text, "discussion", "other", "low", None
#                 )
                
#                 return jsonify({"status": "discussion_created", "summary": summary})
            
#             else:
#                 message_text = f"User shared file: {file_name} ({stratus_url})"
#                 insert_message_into_datastore(
#                     conversation_id, f"file_{message_id}", sender_id, timestamp_ms,
#                     message_text, "discussion", "other", "low", None
#                 )
                
#                 return jsonify({"status": "discussion_created"})
        
#         finally:
#             if os.path.exists(temp_path):
#                 os.remove(temp_path)
#                 print("🗑️ Cleaned up temp file")
    
#     except Exception as e:
#         print(f"❌ Error: {e}")
#         import traceback
#         traceback.print_exc()
#         return jsonify({"error": str(e)}), 500




@app.route('/process_attachment_upload', methods=['POST'])
def process_attachment_upload():
    """Receive attachment uploaded from Deluge Participation Handler"""
    try:
        print("\n" + "="*60)
        print("📎 ATTACHMENT UPLOAD RECEIVED")
        print("="*60)
        
        # Parse metadata from form
        metadata_str = request.form.get('metadata', '{}')
        print(f"Metadata string: {metadata_str}")
        
        # Parse metadata (Deluge sends as string)
        try:
            # Clean Deluge map format
            metadata_str = metadata_str.replace("=", ":")
            metadata = json.loads(metadata_str)
        except:
            # Fallback: try to parse manually
            metadata = {}
            for pair in metadata_str.split(","):
                if "=" in pair:
                    k, v = pair.split("=", 1)
                    metadata[k.strip()] = v.strip()
        
        conversation_id = metadata.get('conversation_id', '')
        sender_id = metadata.get('sender_id', '')
        timestamp_ms = int(metadata.get('timestamp', 0))
        message_id = metadata.get('message_id', '')
        
        print(f"Parsed: conv={conversation_id}, sender={sender_id}, ts={timestamp_ms}")
        
        # Get uploaded file (Deluge sends as 'file')
        uploaded_file = None
        for key in request.files:
            print(f"Found file key: {key}")
            uploaded_file = request.files[key]
            break
        
        if not uploaded_file:
            print("❌ No file found in request")
            return jsonify({"error": "No file uploaded"}), 400
        
        filename = uploaded_file.filename or f"attachment_{uuid.uuid4()}"
        print(f"📎 Processing: {filename}")
        
        # Save to temp file
        ext = filename.split(".")[-1].lower() if "." in filename else "bin"
        temp_path = tempfile.mktemp(suffix=f".{ext}")
        uploaded_file.save(temp_path)
        
        try:
            file_size = os.path.getsize(temp_path)
            print(f"✓ Saved {file_size} bytes to {temp_path}")
            
            # Upload to Stratus
            stratus_key = f"attachments/{message_id}_{filename}"
            stratus_url = upload_to_stratus(temp_path, stratus_key)
            
            if not stratus_url:
                print("❌ Stratus upload failed")
                return jsonify({"error": "Stratus upload failed"}), 500
            
            print(f"✓ Uploaded to Stratus: {stratus_url}")
            
            # Process based on file type
            if ext in ("png", "jpg", "jpeg", "bmp", "webp", "gif"):
                print("🖼️ Processing image with Vision model...")
                analysis = process_image_with_vision(temp_path)
                
                if not analysis:
                    analysis = "Image uploaded (vision analysis unavailable)"
                
                print(f"Vision result: {analysis[:100]}...")
                
                # Check for incident keywords
                analysis_lower = analysis.lower()
                incident_keywords = ["error", "failed", "down", "outage", "critical", 
                                    "production", "timeout", "crash", "exception", "database"]
                
                is_incident = any(kw in analysis_lower for kw in incident_keywords)
                
                if is_incident and "no incident" not in analysis_lower:
                    print("🚨 Incident detected in image!")
                    message_text = f"[Image Analysis] {analysis}"
                    index_message(conversation_id, f"img_{message_id}", sender_id, timestamp_ms, message_text)
                    
                    return jsonify({
                        "status": "incident_created",
                        "analysis": analysis,
                        "stratus_url": stratus_url
                    })
                else:
                    print("💬 Image stored as discussion")
                    message_text = f"User shared image: {stratus_url}\n\nVision analysis: {analysis}"
                    insert_message_into_datastore(
                        conversation_id, f"img_{message_id}", sender_id, timestamp_ms,
                        message_text, "discussion", "other", "low", None
                    )
                    
                    return jsonify({
                        "status": "discussion_created",
                        "analysis": analysis,
                        "stratus_url": stratus_url
                    })
            
            elif ext in ("pdf", "doc", "docx", "txt"):
                print("📄 Processing document with OCR...")
                ocr_text = ocr_document(temp_path, filename)
                
                if ocr_text:
                    print(f"✓ OCR extracted {len(ocr_text)} characters")
                    summary = f"Document '{filename}' contains: {ocr_text[:200]}..."
                else:
                    print("⚠️ OCR failed or empty")
                    summary = f"Document '{filename}' uploaded (OCR unavailable)"
                
                message_text = f"User shared document: {stratus_url}\n\n{summary}"
                insert_message_into_datastore(
                    conversation_id, f"doc_{message_id}", sender_id, timestamp_ms,
                    message_text, "discussion", "other", "low", None
                )
                
                print("📑 Document stored as discussion")
                
                return jsonify({
                    "status": "discussion_created",
                    "summary": summary,
                    "stratus_url": stratus_url
                })
            
            else:
                print(f"📦 Other file type: {ext}")
                message_text = f"User shared file: {filename} ({stratus_url})"
                insert_message_into_datastore(
                    conversation_id, f"file_{message_id}", sender_id, timestamp_ms,
                    message_text, "discussion", "other", "low", None
                )
                
                return jsonify({
                    "status": "discussion_created",
                    "file_type": ext,
                    "stratus_url": stratus_url
                })
        
        finally:
            # Cleanup
            if os.path.exists(temp_path):
                os.remove(temp_path)
                print(f"🗑️ Cleaned up temp file")
    
    except Exception as e:
        print(f"❌ Attachment processing error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

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
    
#     # Skip bot commands - check for bot mention in multiple formats
#     txt_lower = message_text.lower()
    
#     # Skip if message contains bot mention (@workspace-vita or {@b-...})
#     if "@workspace-vita" in txt_lower or "{@b-" in message_text:
#         print(f"Skipping bot command from indexing: {message_text}")
#         return jsonify({"status": "command_skipped"}), 200
    
#     sender_id = user.get("zoho_user_id") or user.get("id")
#     conversation_id = chat.get("id")
    
#     print(f"📨 {message_text!r}")
    
#     index_message(conversation_id, message_id, sender_id, timestamp_ms, message_text)
    
#     return jsonify({"status": "processed"})


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

@app.route('/search_incidents_card_json', methods=['GET'])
def search_incidents_card_json():
    query = request.args.get('query', '').strip()
    
    if not query:
        return jsonify({"results": [], "count": 0})
    
    print("=" * 60)
    print(f"🔍 SEARCH QUERY: '{query}'")
    print("=" * 60)
    
    # Embed query
    q_emb = embed_text(query)
    
    # Search for incidents
    q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    hits = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=q_emb,
        query_filter=q_filter,
        limit=20,
    )
    
    print(f"📊 Qdrant returned {len(hits)} hits")
    
    if not hits:
        return jsonify({"results": [], "count": 0})
    
    # ✅ LOWER threshold to 0.50 for better recall
    filtered_hits = [h for h in hits if h.score >= 0.50]
    
    if not filtered_hits:
        print(f"⚠️ No hits above threshold 0.50")
        for hit in hits[:5]:
            print(f"   Score: {hit.score:.3f} - {hit.payload.get('issue_id', 'N/A')[:12] if hit.payload else 'N/A'}")
        return jsonify({"results": [], "count": 0})
    
    print(f"✅ {len(filtered_hits)} hits above threshold 0.50")
    
    # Show top scores for debugging
    for hit in filtered_hits[:5]:
        print(f"   Score: {hit.score:.3f} - {hit.payload.get('issue_id', 'N/A')[:12] if hit.payload else 'N/A'}")
    
    # Group by issue_id
    issue_scores = {}
    for hit in filtered_hits:
        if hit.payload and (iid := hit.payload.get("issue_id")):
            issue_scores.setdefault(iid, 0)
            issue_scores[iid] = max(issue_scores[iid], hit.score)
    
    # Fetch all issues (both open and resolved)
    all_issues = fetch_all_issues()
    
    results = []
    for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:10]:
        issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
        if not issue:
            continue
        
        # Get messages
        messages = fetch_messages_by_issue_id(issue_id)
        if not messages:
            continue
        
        # Get incident message
        incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
        if not incident_msg:
            continue
        
        title = incident_msg.get("message_text", "Untitled")[:80]
        category = incident_msg.get("category", "other")
        severity = incident_msg.get("severity", "low")
        
        # Status and dates
        status = issue.get("status", "Open")
        opened_at = int(issue.get("opened_at", 0))
        resolved_at = int(issue.get("resolved_at", 0))
        
        try:
            opened_str = datetime.fromtimestamp(opened_at / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_at else "N/A"
        except:
            opened_str = "N/A"
        
        try:
            resolved_str = datetime.fromtimestamp(resolved_at / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_at > 0 else "—"
        except:
            resolved_str = "—"
        
        # Resolution
        if status.lower() == "resolved":
            resolution_summary = issue.get("resolution_summary")
            if not resolution_summary or resolution_summary.strip() == "":
                resolution_summary = "Resolved (no details)"
        else:
            resolution_summary = "🟡 Open - No resolution yet"
        
        results.append({
            "issue_id": issue_id,
            "title": title,
            "status": status,
            "category": category,
            "severity": severity,
            "opened_at": opened_str,
            "resolved_at": resolved_str,
            "resolution_summary": resolution_summary[:150],
            "score": round(score, 2)
        })
    
    print(f"✅ RETURNING {len(results)} results")
    return jsonify({"results": results, "count": len(results)})


# @app.route('/search_incidents_card_json', methods=['GET'])
# def search_incidents_card_json():
#     query = request.args.get('query', '').strip()
    
#     if not query:
#         return jsonify({"results": [], "count": 0})
    
#     print("=" * 60)
#     print(f"🔍 SEARCH QUERY: '{query}'")
#     print("=" * 60)
    
#     # Embed query
#     q_emb = embed_text(query)
    
#     # Search for incidents
#     q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION,
#         query_vector=q_emb,
#         query_filter=q_filter,
#         limit=20,
#     )
    
#     print(f"📊 Qdrant returned {len(hits)} hits")
    
#     if not hits:
#         return jsonify({"results": [], "count": 0})
    
#     # ✅ Filter hits with minimum score threshold of 0.65
#     filtered_hits = [h for h in hits if h.score >= 0.65]
    
#     if not filtered_hits:
#         print(f"⚠️ No hits above threshold 0.65")
#         return jsonify({"results": [], "count": 0})
    
#     print(f"✅ {len(filtered_hits)} hits above threshold")
    
#     # Group by issue_id
#     issue_scores = {}
#     for hit in filtered_hits:
#         if hit.payload and (iid := hit.payload.get("issue_id")):
#             issue_scores.setdefault(iid, 0)
#             issue_scores[iid] = max(issue_scores[iid], hit.score)
    
#     # Fetch all issues
#     all_issues = fetch_all_issues()
    
#     results = []
#     for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:5]:
#         issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
#         if not issue:
#             continue
        
#         # Get messages
#         messages = fetch_messages_by_issue_id(issue_id)
#         if not messages:
#             continue
        
#         # Get incident message
#         incident_msg = next((m for m in messages if m.get("role") == "incident"), None)
#         if not incident_msg:
#             continue
        
#         title = incident_msg.get("message_text", "Untitled")[:80]
#         category = incident_msg.get("category", "other")
#         severity = incident_msg.get("severity", "low")
        
#         # Status and dates
#         status = issue.get("status", "Open")
#         opened_at = int(issue.get("opened_at", 0))
#         resolved_at = int(issue.get("resolved_at", 0))
        
#         opened_str = datetime.fromtimestamp(opened_at / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_at else "N/A"
#         resolved_str = datetime.fromtimestamp(resolved_at / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_at > 0 else "—"
        
#         # Resolution
#         if status.lower() == "resolved":
#             resolution_summary = issue.get("resolution_summary")
#             if not resolution_summary:
#                 resolution_summary = "Resolved (no details)"
#         else:
#             resolution_summary = "🟡 Open - No resolution yet"
        
#         results.append({
#             "issue_id": issue_id,
#             "title": title,
#             "status": status,
#             "category": category,
#             "severity": severity,
#             "opened_at": opened_str,
#             "resolved_at": resolved_str,
#             "resolution_summary": resolution_summary[:150],
#             "score": round(score, 2)
#         })
    
#     print(f"✅ RETURNING {len(results)} results")
#     return jsonify({"results": results, "count": len(results)})


# @app.route('/search_incidents_card_json', methods=['GET'])
# def search_incidents_card_json():
#     query = request.args.get("query", "").strip()
#     if not query:
#         return jsonify({"results": [], "count": 0})
    
#     print(f"\n{'='*60}")
#     print(f"🔍 SEARCH QUERY: '{query}'")
#     print(f"{'='*60}")
    
#     q_emb = embed_text(query)
#     q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    
#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION, 
#         query_vector=q_emb, 
#         query_filter=q_filter, 
#         limit=20,
#     )
    
#     print(f"📊 Qdrant returned {len(hits)} hits")
    
#     if hits:
#         print(f"\n🎯 TOP MATCHES:")
#         for i, hit in enumerate(hits[:5], 1):
#             issue_id = hit.payload.get("issue_id", "N/A")[:12]
#             score = hit.score
#             print(f"  #{i}: score={score:.3f} | issue_id={issue_id}")
    
#     if not hits:
#         return jsonify({"results": [], "count": 0})
    
#     issue_scores = {}
#     for hit in hits:
#         if hit.payload and (iid := hit.payload.get("issue_id")):
#             issue_scores.setdefault(iid, 0)
#             issue_scores[iid] = max(issue_scores[iid], hit.score)
    
#     all_issues = fetch_all_issues()
#     results = []
    
#     for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:5]:
#         issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
#         if not issue:
#             continue
        
#         # Dates
#         opened_ts = int(issue.get("opened_at", 0))
#         opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_ts else "N/A"
        
#         resolved_ts = int(issue.get("resolved_at", 0))
#         status = issue.get("status", "Open")
        
#         # ✅ Properly format resolved date
#         if resolved_ts > 0 and status.lower() == "resolved":
#             resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
#         else:
#             resolved_str = "—"
        
#         # ✅ Get resolution summary (or generate if missing)
#         resolution_summary = issue.get("resolution_summary", "")
        
#         if status.lower() == "resolved" and not resolution_summary:
#             # Generate summary now if missing
#             messages = fetch_messages_by_issue_id(issue_id)
#             resolution_summary = summarize_resolution_with_llm(messages)
#             print(f"🔧 Generated missing summary for {issue_id[:12]}: {resolution_summary[:50]}")
#         elif status.lower() == "open":
#             resolution_summary = "🟡 Open - No resolution yet"
#         elif not resolution_summary:
#             resolution_summary = "No resolution recorded"
        
#         results.append({
#             "issue_id": issue_id,  # ✅ Include full issue_id for buttons
#             "title": issue.get("title", "Untitled")[:80],
#             "status": status,
#             "category": issue.get("category", "other"),
#             "severity": issue.get("severity", "low"),
#             "opened_at": opened_str,
#             "resolved_at": resolved_str,  # ✅ Now shows actual date
#             "resolution_summary": resolution_summary[:150],
#             "score": round(score, 2)
#         })
    
#     print(f"\n✅ RETURNING {len(results)} results")
#     return jsonify({"results": results, "count": len(results)})


# @app.route('/search_incidents_card_json', methods=['GET'])
# def search_incidents_card_json():
#     query = request.args.get("query", "").strip()
#     if not query:
#         return jsonify({"results": [], "count": 0})
    
#     print(f"\n{'='*60}")
#     print(f"🔍 SEARCH QUERY: '{query}'")
#     print(f"{'='*60}")
    
#     # Embed & search incidents
#     q_emb = embed_text(query)
#     print(f"✅ Embedded query (vector size: {len(q_emb)})")
    
#     q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
    
#     # ✅ LOWER THRESHOLD AND INCREASE LIMIT
#     hits = qdrant.search(
#         collection_name=QDRANT_COLLECTION, 
#         query_vector=q_emb, 
#         query_filter=q_filter, 
#         limit=20,  # More results
#         score_threshold=0.3  # ✅ Lower threshold (30% match)
#     )
    
#     print(f"📊 Qdrant returned {len(hits)} hits")
    
#     if hits:
#         print(f"\n🎯 TOP MATCHES:")
#         for i, hit in enumerate(hits[:5], 1):
#             issue_id = hit.payload.get("issue_id", "N/A")[:12]
#             score = hit.score
#             print(f"  #{i}: score={score:.3f} | issue_id={issue_id}")
#     else:
#         print("❌ NO HITS FOUND!")
#         # Debug: Try searching WITHOUT filter to see if ANY results exist
#         all_hits = qdrant.search(
#             collection_name=QDRANT_COLLECTION,
#             query_vector=q_emb,
#             limit=5
#         )
#         print(f"🔍 Search without filter found {len(all_hits)} results:")
#         for hit in all_hits:
#             print(f"  - role={hit.payload.get('role')}, score={hit.score:.3f}")
    
#     if not hits:
#         return jsonify({"results": [], "count": 0})
    
#     # Get scored issue_ids (closest matches first)
#     issue_scores = {}
#     for hit in hits:
#         if hit.payload and (iid := hit.payload.get("issue_id")):
#             issue_scores.setdefault(iid, 0)
#             issue_scores[iid] = max(issue_scores[iid], hit.score)
    
#     print(f"\n📋 Unique issues found: {len(issue_scores)}")
    
#     # Fetch ALL issues & match scored ones
#     all_issues = fetch_all_issues()
#     print(f"✅ Fetched {len(all_issues)} total issues from DataStore")
    
#     results = []
    
#     for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:5]:
#         issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
#         if not issue:
#             print(f"⚠️ Issue {issue_id[:12]} found in Qdrant but NOT in DataStore!")
#             continue
        
#         # Dates
#         opened_ts = int(issue.get("opened_at", 0))
#         opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_ts else "N/A"
#         resolved_ts = int(issue.get("resolved_at", 0))
#         resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_ts else "Open"
        
#         # Resolution summary
#         resolution_summary = issue.get("resolution_summary", "No resolution recorded")
#         if issue.get("status", "").lower() == "open":
#             resolution_summary = "🟡 Open - No resolution yet"
        
#         results.append({
#             "title": issue.get("title", "Untitled")[:80],
#             "status": issue.get("status", "Open"),
#             "category": issue.get("category", "other"),
#             "severity": issue.get("severity", "low"),
#             "opened_at": opened_str,
#             "resolved_at": resolved_str,
#             "resolution_summary": resolution_summary[:150],
#             "score": round(score, 2)
#         })
        
#         print(f"✅ Added result: {issue.get('title', '')[:50]} (score={score:.2f})")
    
#     print(f"\n✅ RETURNING {len(results)} results")
#     print(f"{'='*60}\n")
    
#     return jsonify({"results": results, "count": len(results)})


# @app.route('/search_incidents_card_json', methods=['GET'])
# def search_incidents_card_json():
#     query = request.args.get("query", "").strip()
#     if not query:
#         return jsonify({"results": [], "count": 0})
    
#     print(f"🔍 Card search: {query}")
    
#     # Embed & search incidents
#     q_emb = embed_text(query)
#     q_filter = Filter(must=[FieldCondition(key="role", match=MatchValue(value="incident"))])
#     hits = qdrant.search(collection_name=QDRANT_COLLECTION, query_vector=q_emb, query_filter=q_filter, limit=10)
    
#     if not hits:
#         return jsonify({"results": [], "count": 0})
    
#     # Get scored issue_ids (closest matches first)
#     issue_scores = {}
#     for hit in hits:
#         if hit.payload and (iid := hit.payload.get("issue_id")):
#             issue_scores.setdefault(iid, 0)
#             issue_scores[iid] = max(issue_scores[iid], hit.score)
    
#     # Fetch ALL issues & match scored ones (latest first within scores)
#     all_issues = fetch_all_issues()
#     results = []
    
#     for issue_id, score in sorted(issue_scores.items(), key=lambda x: x[1], reverse=True)[:5]:
#         issue = next((i for i in all_issues if i.get("issue_id") == issue_id), None)
#         if not issue:
#             continue
        
#         # Dates
#         opened_ts = int(issue.get("opened_at", 0))
#         opened_str = datetime.fromtimestamp(opened_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if opened_ts else "N/A"
#         resolved_ts = int(issue.get("resolved_at", 0))
#         resolved_str = datetime.fromtimestamp(resolved_ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M") if resolved_ts else "Open"
        
#         # Resolution summary
#         resolution_summary = issue.get("resolution_summary", "No resolution recorded")
#         if issue.get("status", "").lower() == "open":
#             resolution_summary = "🟡 Open - No resolution yet"
        
#         results.append({
#             "title": issue.get("title", "Untitled")[:80],
#             "status": issue.get("status", "Open"),
#             "category": issue.get("category", "other"),
#             "severity": issue.get("severity", "low"),
#             "opened_at": opened_str,
#             "resolved_at": resolved_str,
#             "resolution_summary": resolution_summary[:150],
#             "score": round(score, 2)
#         })
    
#     return jsonify({"results": results, "count": len(results)})

def store_resolution_summary(issue_id: str, summary: str, resolved_at_ms: int) -> bool:
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return False

    base_url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {"Authorization": f"Zoho-oauthtoken {get_catalyst_token()}", "Content-Type": "application/json", "CATALYST-ORG": CATALYST_ORG_ID}

    try:
        # Find issue ROWID
        resp = requests.get(f"{base_url}?max_rows=300", headers=headers, timeout=10)
        if resp.status_code != 200:
            return False
        
        rows = resp.json().get("data", [])
        issue_row = next((r for r in rows if r.get("issue_id") == issue_id), None)
        if not issue_row:
            return False  
        
        row_id = issue_row.get("ROWID")
        update_body = [{
            "ROWID": row_id,
            "status": "Resolved",
            "resolved_at": str(int(resolved_at_ms)),
            "resolution_summary": summary[:500]  # ✅ Stores LLM summary here
        }]
        
        resp = requests.put(base_url, headers=headers, json=update_body, timeout=10)
        print(f"✅ Resolution stored for {issue_id}: {resp.status_code}")
        return resp.status_code == 200
        
    except Exception as e:
        print(f"❌ Resolution store failed: {e}")
        return False

@app.route('/debug_qdrant', methods=['GET'])
def debug_qdrant():
    """Check what's in Qdrant"""
    try:
        # Get all points
        result = qdrant.scroll(
            collection_name=QDRANT_COLLECTION,
            limit=50,  # ✅ Increased to get more
            with_payload=True,
            with_vectors=False
        )
        
        points = result[0]
        
        debug_info = {
            "total_points": len(points),
            "incidents": [],
            "discussions": [],
            "resolutions": []
        }
        
        # Also fetch messages from DataStore to get text
        for point in points:
            role = point.payload.get("role", "unknown")
            issue_id = point.payload.get("issue_id", "none")
            message_id = point.payload.get("message_id", "")
            
            item = {
                "role": role,
                "issue_id": issue_id[:12] + "..." if issue_id else "none",
                "message_id": message_id,
            }
            
            if role == "incident":
                debug_info["incidents"].append(item)
            elif role == "discussion":
                debug_info["discussions"].append(item)
            elif role == "resolution":
                debug_info["resolutions"].append(item)
        
        # ✅ NOW fetch the actual incident messages from DataStore
        if debug_info["incidents"]:
            url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
            headers = {
                "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
                "CATALYST-ORG": CATALYST_ORG_ID,
            }
            
            try:
                resp = requests.get(f"{url}?max_rows=100", headers=headers, timeout=10)
                if resp.status_code == 200:
                    all_messages = resp.json().get("data", [])
                    
                    # Match incidents with their text
                    for item in debug_info["incidents"]:
                        msg_id = item["message_id"]
                        msg = next((m for m in all_messages if m.get("message_id") == msg_id), None)
                        if msg:
                            item["text"] = msg.get("message_text", "N/A")[:100]  # First 100 chars
                            item["category"] = msg.get("category", "N/A")
            except Exception as e:
                debug_info["error"] = f"Failed to fetch messages: {e}"
        
        return jsonify(debug_info)
        
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/clear_qdrant', methods=['POST'])
def clear_qdrant():
    """Delete all points and recreate collection with CORRECT dimensions"""
    try:
        # Delete collection
        try:
            qdrant.delete_collection(collection_name=QDRANT_COLLECTION)
            print(f"✅ Deleted collection {QDRANT_COLLECTION}")
        except Exception as e:
            print(f"Collection didn't exist or already deleted: {e}")
        
        # ✅ Get correct dimension from a test embedding
        test_emb = embed_text("test")
        vector_dim = len(test_emb)
        print(f"✅ Detected vector dimension: {vector_dim}")
        
        # Recreate with CORRECT dimension
        qdrant.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),  # ✅ Use actual dimension
        )
        
        # Recreate indexes
        for field in ["role", "category", "issue_id"]:
            try:
                qdrant.create_payload_index(
                    collection_name=QDRANT_COLLECTION,
                    field_name=field,
                    field_schema=PayloadSchemaType.KEYWORD,
                )
            except:
                pass
        
        print(f"✅ Recreated collection {QDRANT_COLLECTION} with dimension {vector_dim}")
        return jsonify({
            "status": "success", 
            "message": f"Qdrant cleared and recreated with dimension {vector_dim}"
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)})


@app.route('/reindex_all', methods=['POST'])
def reindex_all():
    """Re-index all messages from DataStore into Qdrant"""
    
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return jsonify({"error": "Missing Catalyst config"})
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    all_messages = []
    next_token = None
    
    try:
        # Fetch ALL messages from DataStore
        while True:
            params = "max_rows=300"
            if next_token:
                params += f"&next_token={next_token}"
            
            resp = requests.get(f"{url}?{params}", headers=headers, timeout=10)
            if resp.status_code != 200:
                break
            
            body = resp.json()
            rows = body.get("data", [])
            all_messages.extend(rows)
            
            next_token = body.get("next_token")
            if not next_token:
                break
        
        print(f"📥 Fetched {len(all_messages)} messages from DataStore")
        
        # Index each message into Qdrant
        indexed_count = 0
        
        for msg in all_messages:
            message_text = msg.get("message_text", "")
            message_id = msg.get("message_id", "")
            
            if not message_text or not message_id:
                continue
            
            # Embed
            emb = embed_text(message_text)
            
            # Create point
            qdrant_id = normalize_message_id(message_id)
            point = PointStruct(
                id=qdrant_id,
                vector=emb,
                payload={
                    "conversation_id": msg.get("conversation_id", ""),
                    "sender_id": msg.get("sender_id", ""),
                    "role": msg.get("role", "discussion"),
                    "category": msg.get("category", "other"),
                    "severity": msg.get("severity", "low"),
                    "issue_id": msg.get("issue_id", ""),
                    "message_id": message_id,
                },
            )
            
            qdrant.upsert(QDRANT_COLLECTION, [point])
            indexed_count += 1
            
            if indexed_count % 10 == 0:
                print(f"  Indexed {indexed_count}/{len(all_messages)}...")
        
        print(f"✅ Successfully indexed {indexed_count} messages")
        return jsonify({
            "status": "success",
            "total_messages": len(all_messages),
            "indexed": indexed_count
        })
        
    except Exception as e:
        print(f"❌ Reindex error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)})


@app.route('/clear_conversations', methods=['POST'])
def clear_conversations():
    """Delete all rows from conversations table"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return jsonify({"error": "Missing Catalyst config"})
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    try:
        # 1. Fetch all ROWIDs
        all_rows = []
        next_token = None
        
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
        
        print(f"📥 Found {len(all_rows)} conversation rows to delete")
        
        # 2. Delete in batches (max 100 per request)
        deleted_count = 0
        batch_size = 100
        
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i+batch_size]
            row_ids = [str(row.get("ROWID")) for row in batch if row.get("ROWID")]
            
            if not row_ids:
                continue
            
            # Delete using query params (Catalyst format)
            delete_url = f"{url}?ids={','.join(row_ids)}"
            del_resp = requests.delete(delete_url, headers=headers, timeout=10)
            
            if del_resp.status_code == 200:
                deleted_count += len(row_ids)
                print(f"  Deleted {deleted_count}/{len(all_rows)}...")
            else:
                print(f"❌ Delete failed: {del_resp.status_code} {del_resp.text}")
        
        print(f"✅ Deleted {deleted_count} conversation rows")
        return jsonify({
            "status": "success",
            "table": "conversations",
            "deleted": deleted_count
        })
        
    except Exception as e:
        print(f"❌ Clear conversations error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)})


@app.route('/clear_issues', methods=['POST'])
def clear_issues():
    """Delete all rows from issues table"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return jsonify({"error": "Missing Catalyst config"})
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{ISSUES_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    try:
        # 1. Fetch all ROWIDs
        all_rows = []
        next_token = None
        
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
        
        print(f"📥 Found {len(all_rows)} issue rows to delete")
        
        # 2. Delete in batches
        deleted_count = 0
        batch_size = 100
        
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i+batch_size]
            row_ids = [str(row.get("ROWID")) for row in batch if row.get("ROWID")]
            
            if not row_ids:
                continue
            
            delete_url = f"{url}?ids={','.join(row_ids)}"
            del_resp = requests.delete(delete_url, headers=headers, timeout=10)
            
            if del_resp.status_code == 200:
                deleted_count += len(row_ids)
                print(f"  Deleted {deleted_count}/{len(all_rows)}...")
            else:
                print(f"❌ Delete failed: {del_resp.status_code} {del_resp.text}")
        
        print(f"✅ Deleted {deleted_count} issue rows")
        return jsonify({
            "status": "success",
            "table": "issues",
            "deleted": deleted_count
        })
        
    except Exception as e:
        print(f"❌ Clear issues error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)})


@app.route('/clear_all_data', methods=['POST'])
def clear_all_data():
    """Clear Qdrant + Conversations + Issues (FULL RESET)"""
    results = {}
    
    # 1. Clear Qdrant
    try:
        qdrant.delete_collection(collection_name=QDRANT_COLLECTION)
        
        # Get dimension
        test_emb = embed_text("test")
        vector_dim = len(test_emb)
        
        # Recreate
        qdrant.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=vector_dim, distance=Distance.COSINE),
        )
        
        for field in ["role", "category", "issue_id"]:
            try:
                qdrant.create_payload_index(
                    collection_name=QDRANT_COLLECTION,
                    field_name=field,
                    field_schema=PayloadSchemaType.KEYWORD,
                )
            except:
                pass
        
        results["qdrant"] = "cleared"
        print("✅ Cleared Qdrant")
    except Exception as e:
        results["qdrant"] = f"error: {e}"
    
    # 2. Clear Conversations
    conv_result = clear_conversations()
    results["conversations"] = conv_result.json
    
    # 3. Clear Issues
    issues_result = clear_issues()
    results["issues"] = issues_result.json
    
    return jsonify(results)


@app.route('/debug_last_messages', methods=['GET'])
def debug_last_messages():
    """Show last 10 messages from DataStore with their classifications"""
    if not (CATALYST_TOKEN and CATALYST_PROJECT_ID):
        return jsonify({"error": "Missing config"})
    
    url = f"https://api.catalyst.zoho.com/baas/v1/project/{CATALYST_PROJECT_ID}/table/{CONVERSATIONS_TABLE}/row"
    headers = {
        "Authorization": f"Zoho-oauthtoken {get_catalyst_token()}",
        "CATALYST-ORG": CATALYST_ORG_ID,
    }
    
    try:
        resp = requests.get(f"{url}?max_rows=20", headers=headers, timeout=10)
        if resp.status_code != 200:
            return jsonify({"error": "Failed to fetch"})
        
        rows = resp.json().get("data", [])
        
        # Sort by timestamp descending (newest first)
        rows.sort(key=lambda r: int(r.get("time_stamp", 0)), reverse=True)
        
        results = []
        for row in rows[:10]:
            ts = int(row.get("time_stamp", 0))
            dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            
            results.append({
                "time": dt,
                "text": row.get("message_text", "")[:80],
                "role": row.get("role", "N/A"),
                "category": row.get("category", "N/A"),
                "severity": row.get("severity", "N/A"),
                "issue_id": row.get("issue_id", "none")[:12] + "..." if row.get("issue_id") else "none"
            })
        
        return jsonify({"last_messages": results})
        
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/admin/refresh_token', methods=['POST'])
def admin_refresh_token():
    """Manually trigger token refresh"""
    success = token_manager.refresh_access_token()
    
    if success:
        return jsonify({
            "status": "success",
            "message": "Token refreshed",
            "token_preview": get_catalyst_token()[:20] + "..."
        })
    else:
        return jsonify({
            "status": "error",
            "message": "Token refresh failed"
        }), 500


@app.route('/admin/token_status', methods=['GET'])
def admin_token_status():
    """Check current token status"""
    return jsonify({
        "token_preview": get_catalyst_token()[:20] + "..." if get_catalyst_token() else "NOT SET",
        "refresh_token_available": bool(token_manager.refresh_token),
        "auto_refresh_enabled": bool(token_manager.refresh_token)
    })




if __name__ == '__main__':
    # ✅ Start auto token refresh
    token_manager.start_auto_refresh()
    
    print("\n" + "="*60)
    print("🚀 Starting Workspace-vita Backend")
    print("="*60)
    print(f"📋 Project: {CATALYST_PROJECT_ID}")
    print(f"🏢 Org: {CATALYST_ORG_ID}")
    print(f"🔐 Token: {get_catalyst_token()[:20] if get_catalyst_token() else 'NOT SET'}...")
    print("="*60 + "\n")
    
    app.run(port=8000, debug=True, use_reloader=False)  # use_reloader=False important for scheduler
