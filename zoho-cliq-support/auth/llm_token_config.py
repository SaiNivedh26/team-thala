from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, redirect
import os
import requests

app = Flask(__name__)
app.secret_key = "YOUR_SECRET_KEY"  # change this

# ---- Config: fill these from Zoho Developer Console ----
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Must be exactly the same as in Developer Console
REDIRECT_URI = "http://localhost:9000/oauth/callback"

# Use the IN accounts server since your org is in India
ACCOUNTS_BASE = "https://accounts.zoho.in"

AUTH_URL = f"{ACCOUNTS_BASE}/oauth/v2/auth"
TOKEN_URL = f"{ACCOUNTS_BASE}/oauth/v2/token"

# TODO: replace with the exact scopes QuickML requires in your DC
# Example pattern: "ZohoCatalyst.quickml.ALL" + profile/basic scope if needed


# QuickML external REST endpoints use this scope name
SCOPE = "QuickML.deployment.READ ZohoProfile.userinfo.read"

# Your Catalyst org and project
ORG_ID = os.getenv("ORG_ID")
PROJECT_ID = os.getenv("PROJECT_ID")

# Global variables just for demo
ACCESS_TOKEN = None
REFRESH_TOKEN = None


@app.route("/")
def index():
    return "Go to /login to start OAuth flow."


@app.route("/login")
def login():
    params = {
        "client_id": CLIENT_ID,
        "response_type": "code",
        "scope": SCOPE,
        "redirect_uri": REDIRECT_URI,
        "access_type": "offline",
        "prompt": "consent",
    }
    # Build auth URL and redirect browser there
    req = requests.Request("GET", AUTH_URL, params=params).prepare()
    return redirect(req.url)


@app.route("/oauth/callback")
def oauth_callback():
    global ACCESS_TOKEN, REFRESH_TOKEN

    code = request.args.get("code")
    if not code:
        return "Missing authorization code in callback", 400

    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "redirect_uri": REDIRECT_URI,
    }

    token_resp = requests.post(TOKEN_URL, data=data)
    token_json = token_resp.json()
    print("Token response:", token_json)

    if "access_token" not in token_json:
        return f"Failed to get access token: {token_json}", 400

    ACCESS_TOKEN = token_json["access_token"]
    REFRESH_TOKEN = token_json.get("refresh_token")

    # Now call QuickML once as a sample
    quickml_result = call_quickml_chat(ACCESS_TOKEN)

    return f"""
    <h3>OAuth Success</h3>
    <p>Access token (use as CATALYST_TOKEN):</p>
    <pre>{ACCESS_TOKEN}</pre>
    <p>Refresh token:</p>
    <pre>{REFRESH_TOKEN}</pre>
    <h3>QuickML sample response:</h3>
    <pre>{quickml_result}</pre>
    """


def call_quickml_chat(access_token: str) -> str:
    """
    Calls the QuickML LLM chat endpoint once using the given access token.
    Returns the response text (JSON string).
    """

    url = f"https://api.catalyst.zoho.in/quickml/v2/project/{PROJECT_ID}/llm/chat"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "CATALYST-ORG": ORG_ID,
    }
    body = {
        "prompt": "Tell me more about quantum computing and its future implications",
        "model": "crm-di-qwen_text_14b-fp8-it",
        "system_prompt": "Be concise and factual",
        "top_p": 0.9,
        "top_k": 50,
        "best_of": 1,
        "temperature": 0.7,
        "max_tokens": 256,
    }

    resp = requests.post(url, json=body, headers=headers, timeout=60)
    try:
        return resp.text
    except Exception:
        return f"Status {resp.status_code}, raw body: {resp.content}"


if __name__ == "__main__":
    # Run small local server on port 9000 just for OAuth redirect
    app.run(port=9000, debug=True)
