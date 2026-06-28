from flask import Flask, request, jsonify, redirect
import requests
import os
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')
REDIRECT_URI = 'http://localhost:8000/oauth/callback'
SCOPE = 'ZohoCliq.Channels.READ,ZohoCliq.Messages.READ,ZohoCliq.Messages.CREATE'
AUTH_URL = 'https://accounts.zoho.com/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.com/oauth/v2/token'

# Store tokens in memory (use DB for production)
ACCESS_TOKEN = None
REFRESH_TOKEN = None

# Step 1: Redirect to Zoho OAuth for user consent
@app.route('/login')
def login():
    params = {
        'scope': SCOPE,
        'client_id': CLIENT_ID,
        'response_type': 'code',
        'access_type': 'offline',
        'redirect_uri': REDIRECT_URI,
        'prompt': 'consent'
    }
    url = requests.Request('GET', AUTH_URL, params=params).prepare().url
    return redirect(url)

# Step 2: OAuth callback to receive authorization code and get tokens
@app.route('/oauth/callback')
def oauth_callback():
    global ACCESS_TOKEN, REFRESH_TOKEN
    code = request.args.get('code')
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
    return "OAuth Successful! You can now receive events."

# Step 3: Bot Participation Handler endpoint to receive events from Zoho Cliq
@app.route('/bot/events', methods=['POST'])
def bot_events():
    global ACCESS_TOKEN
    event_data = request.get_json()
    
    # Example: Reply to message.create event
    if event_data.get('event') == 'message.create':
        message = event_data['message']
        channel = message['channel']
        text = message['text']
        
        reply_text = f"Received your message: {text}"

        # Post reply message back to channel
        post_url = f"https://cliq.zoho.com/api/v2/channels/{channel}/message"
        headers = {
            'Authorization': f'Zoho-oauthtoken {ACCESS_TOKEN}',
            'Content-Type': 'application/json'
        }
        payload = {
            "text": reply_text
        }
        requests.post(post_url, headers=headers, json=payload)
    
    return jsonify({"status": "ok"})

if __name__ == '__main__':
    app.run(port=8000, debug=True)
