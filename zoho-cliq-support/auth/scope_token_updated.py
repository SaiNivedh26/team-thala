from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, redirect
import os
import requests

app = Flask(__name__)
app.secret_key = "YOUR_SECRET_KEY"

CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
REDIRECT_URI = "http://localhost:9000/oauth/callback"

ACCOUNTS_BASE = "https://accounts.zoho.com"
AUTH_URL = f"{ACCOUNTS_BASE}/oauth/v2/auth"
TOKEN_URL = f"{ACCOUNTS_BASE}/oauth/v2/token"

# CORRECT SCOPE for OCR from the docs: ZohoCatalyst.mlkit.READ
SCOPE = "QuickML.deployment.READ,ZohoCatalyst.mlkit.READ,ZohoProfile.userinfo.read,ZohoCatalyst.buckets.READ,Stratus.fileop.CREATE,ZohoCatalyst.tables.rows.CREATE,ZohoCatalyst.tables.rows.READ,ZohoCatalyst.tables.rows.UPDATE,ZohoCatalyst.tables.READ,ZohoCatalyst.tables.READ,ZohoCatalyst.tables.columns.READ,ZohoCatalyst.tables.columns.READ,ZohoCatalyst.cache.CREATE,ZohoCatalyst.cache.CREATE,ZohoCatalyst.tables.rows.DELETE"


ORG_ID = os.getenv("ORG_ID")
PROJECT_ID = os.getenv("PROJECT_ID")

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

    return f"""
    <h3>OAuth Success</h3>
    <p>Access token (use as CATALYST_TOKEN):</p>
    <pre>{ACCESS_TOKEN}</pre>
    <p>Refresh token:</p>
    <pre>{REFRESH_TOKEN}</pre>
    """


if __name__ == "__main__":
    app.run(port=9000, debug=True)
