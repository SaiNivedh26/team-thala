import os
import requests
from dotenv import load_dotenv
load_dotenv()

PROJECT_ID = os.getenv("CATALYST_PROJECT_ID")          # your project id
ORG_ID = os.getenv("CATALYST_ORG_ID")                    # your org id
ACCESS_TOKEN = os.getenv("CATALYST_TOKEN")  # put your OAuth/Zoho access token in env

url = f"https://api.catalyst.zoho.com/quickml/v2/project/{PROJECT_ID}/llm/chat"

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "CATALYST-ORG": ORG_ID,
}

data = {
    "prompt": "Just give one shot answer if it belongs as incidental or discussion or resolution. The Statement is The issue related to deployment has been fixed",
    "model": "crm-di-qwen_text_14b-fp8-it",
    "system_prompt": "Be concise and factual",
    "top_p": 0.9,
    "top_k": 50,
    "best_of": 1,
    "temperature": 0.7,
    "max_tokens": 256,
}

resp = requests.post(url, json=data, headers=headers, timeout=60)
print("Status:", resp.status_code)
print("Body:", resp.json())
