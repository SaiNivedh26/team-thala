from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect, session
import requests
import os

app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # Replace with your secret key

# Your Zoho OAuth client credentials
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/oauth/callback'  # Must match Zoho developer console

AUTH_URL = 'https://accounts.zoho.in/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.in/oauth/v2/token'

# Scopes your bot needs (adjust as necessary)
SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'


# Step 1: Redirect user to Zoho auth page to get authorization code
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


# Step 2: OAuth callback to get access and refresh tokens
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

    # Check for error in the response
    if 'access_token' not in token_json:
        # Provide better debug info
        error = token_json.get('error', 'Unknown error')
        error_description = token_json.get('error_description', '')
        return f"Failed to get access token: {error} - {error_description}", 400

    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful! You can now receive events."


# Function to refresh access token when expired
def refresh_access_token():
    refresh_token = session.get('refresh_token')
    if not refresh_token:
        return None

    data = {
        'refresh_token': refresh_token,
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type': 'refresh_token'
    }
    response = requests.post(TOKEN_URL, data=data)
    if response.status_code == 200:
        tokens = response.json()
        session['access_token'] = tokens['access_token']
        return tokens['access_token']
    return None


# Bot Participation Handler endpoint
@app.route('/participation', methods=['POST'])
def participation_handler():
    event = request.get_json()
    print("Received event:", event)

    access_token = session.get('access_token')
    if not access_token:
        return jsonify({"error": "Unauthorized, please /login"}), 401

    # Handle token expiration by refreshing (optional)
    # Optionally you can detect 401 from Zoho API and refresh token here

    if event.get('event') == 'message.create':
        message = event['message']
        channel_id = message['channel']
        user_text = message['text']

        reply_text = f"You said: {user_text}"

        post_url = f"https://cliq.zoho.com/api/v2/channels/{channel_id}/message"
        headers = {
            'Authorization': f'Zoho-oauthtoken {access_token}',
            'Content-Type': 'application/json'
        }
        payload = {"text": reply_text}

        res = requests.post(post_url, headers=headers, json=payload)
        if res.status_code == 401:
            # Try refreshing token and retry once
            new_token = refresh_access_token()
            if new_token:
                headers['Authorization'] = f'Zoho-oauthtoken {new_token}'
                res = requests.post(post_url, headers=headers, json=payload)

        if res.status_code != 200:
            print("Failed to send message:", res.text)

    return jsonify({"status": "received"})

@app.route('/bot/events', methods=['POST'])
def bot_events():
    # Deluge invokeurl sends data as form fields
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    print(form_data)

    # Example: extract some useful fields
    operation = form_data.get('operation')
    print("Operation:", operation)

    # user, chat, data will usually come as stringified maps; just print first
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    data_raw = form_data.get('data')

    print("User raw:", user_raw)
    print("Chat raw:", chat_raw)
    print("Data raw:", data_raw)

    # You can later parse these strings (JSON / key-value) if needed

    return jsonify({"status": "ok"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
