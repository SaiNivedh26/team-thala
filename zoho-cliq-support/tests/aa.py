from dotenv import load_dotenv
load_dotenv()

from flask import Flask, request, jsonify, redirect, session
import requests
import os
import json

app = Flask(__name__)
app.secret_key = 'YOUR_SECRET_KEY'  # Replace with your secret key

# ===== Zoho OAuth client credentials (for sending messages back to Cliq, optional) =====
CLIENT_ID = os.getenv('CLIENT_ID')
CLIENT_SECRET = os.getenv('CLIENT_SECRET')

REDIRECT_URI = 'http://localhost:8000/oauth/callback'  # Must match Zoho developer console
AUTH_URL = 'https://accounts.zoho.com/oauth/v2/auth'
TOKEN_URL = 'https://accounts.zoho.com/oauth/v2/token'
SCOPE = 'ZohoCliq.Channels.READ ZohoCliq.Messages.READ ZohoCliq.Messages.CREATE'

ACCESS_TOKEN = None
REFRESH_TOKEN = None

# ===== Zoho Signals Event REST API URL (from cliq_message_event REST API panel) =====
# Example:
# SIGNALS_EVENT_URL = "https://api.catalyst.zoho.in/signals/v2/event_notification?digest=..."
SIGNALS_EVENT_URL = os.getenv('SIGNALS_EVENT_URL')


# ----------------- OAuth (for later, if you want to call Cliq REST APIs) -----------------

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


# ----------------- Producer: Deluge → Flask → Signals -----------------

@app.route('/bot/events', methods=['POST'])
def bot_events():
    # Deluge invokeurl sends payload as form fields
    form_data = request.form.to_dict()
    print("---- Received from Deluge ----")
    print(form_data)

    operation = form_data.get('operation')
    user_raw = form_data.get('user')
    chat_raw = form_data.get('chat')
    data_raw = form_data.get('data')

    # Inner data that we want to end up in events[0].data.data in Signals
    inner_data = {
        "source": "zoho_cliq_bot",
        "operation": operation,
        "user": user_raw,
        "chat": chat_raw,
        "raw": data_raw
    }

    # Wrap as { "data": { ... } } for the event_notification API
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


# ----------------- Consumer: Signals queue → Flask -----------------

@app.route('/signals/consume', methods=['POST'])
def signals_consume():
    payload = request.get_json()
    print("==== Signals delivered queued event ====")
    print(payload)

    events = payload.get("events", [])
    if events:
        event_obj = events[0]

        # Outer level: event_obj["data"] is what Signals stores
        outer_data = event_obj.get("data", {})          # {'data': {...}}
        inner_data = outer_data.get("data", {})         # {... with source, operation, user, chat, raw}

        print("---- Inner event data ----")
        print(inner_data)

        source = inner_data.get("source")
        operation = inner_data.get("operation")
        print("Source:", source)
        print("Operation:", operation)

        # These are JSON strings from Deluge; parse them to access fields
        raw_str = inner_data.get("raw")
        user_str = inner_data.get("user")
        chat_str = inner_data.get("chat")

        if raw_str:
            try:
                raw = json.loads(raw_str)
                message_text = raw["message"]["content"]["text"]
                print("Message text:", message_text)
            except Exception as e:
                print("Error parsing raw message JSON:", e)

        if user_str:
            try:
                user = json.loads(user_str)
                print("User email:", user.get("email"))
            except Exception as e:
                print("Error parsing user JSON:", e)

        if chat_str:
            try:
                chat = json.loads(chat_str)
                print("Channel unique name:", chat.get("channel_unique_name"))
            except Exception as e:
                print("Error parsing chat JSON:", e)

    return jsonify({"status": "processed"})


if __name__ == '__main__':
    app.run(port=8000, debug=True)
