from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect, session
import requests
import os

app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # Replace with your secret key

# ===== Zoho OAuth client credentials (for sending to Cliq, optional for now) =====
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/oauth/callback'
AUTH_URL = 'https://accounts.zoho.in/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.in/oauth/v2/token'
SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

ACCESS_TOKEN = None
REFRESH_TOKEN = None

# ===== Signals Event REST API URL (from your event REST API panel) =====
SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')


# ----------------- OAuth (unchanged) -----------------

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
        error = token_json.get('error', 'Unknown error')
        error_description = token_json.get('error_description', '')
        return f"Failed to get access token: {error} - {error_description}", 400

    ACCESS_TOKEN = token_json.get('access_token')
    REFRESH_TOKEN = token_json.get('refresh_token')
    return "OAuth Successful! You can now send messages to Cliq using the REST API."


# ----------------- Producer: Deluge -> Flask -> Signals -----------------

@app.route('/bot/events', methods=['POST'])
def bot_events():
    # Deluge invokeurl sends data as form fields
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    print(form_data)

    operation = form_data.get('operation')
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    data_raw = form_data.get('data')

    # Inner data that will end up inside events[0].data in Signals
    inner_data = {
        "source": "zoho_cliq_bot",
        "operation": operation,
        "user": user_raw,
        "chat": chat_raw,
        "raw": data_raw
    }

    # Wrap as { "data": { ... } } for event_notification API
    event_payload = {
        "data": inner_data
    }

    if SIGNALS_EVENT_URL:
        try:
            resp = requests.post(
                SIGNALS_EVENT_URL,
                headers={"Content-Type": "application/json"},
                json=event_payload
            )
            print("Signals event_notification response:", resp.status_code, resp.text)
        except Exception as e:
            print("Error publishing to Signals:", e)
    else:
        print("SIGNALS_EVENT_URL not set, skipping publish to Signals.")

    return jsonify({"status": "ok"})


# ----------------- Consumer: Signals -> Flask (webhook target) -----------------

@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    print(payload)

    # Your sample shows data inside events[0].data
    events = payload.get("events", [])
    if events:
        event_obj = events[0]
        data = event_obj.get("data", {})
        print("---- Inner event data ----")
        print(data)

        source = data.get("source")
        operation = data.get("operation")
        print("Source:", source)
        print("Operation:", operation)

    return jsonify({"status": "processed"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
