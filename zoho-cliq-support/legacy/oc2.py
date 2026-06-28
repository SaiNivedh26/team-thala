import os
import json
import requests
from dotenv import load_dotenv
load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
ACCESS_TOKEN = os.getenv("CATALYST_TOKEN")  # your Zoho OAuth access token

def run_ocr():
    if not ACCESS_TOKEN:
        raise RuntimeError("Set CATALYST_TOKEN env var to a valid Zoho OAuth token")

    url = f"https://api.catalyst.zoho.com/baas/v1/project/{PROJECT_ID}/ml/ocr"

    headers = {
        # Use Zoho-oauthtoken, same as curl example
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
        # Do NOT manually set Content-Type; requests will handle boundary
    }

    with open("sample.png", "rb") as f:
        files = {
            # field name is "image" as in the curl: -F "image=..."
            "image": ("sample.png", f, "image/png"),
        }
        data = {
            "language": "eng",   # or "eng,spa" etc.
            # modelType is optional in this older OCR endpoint; language is enough
        }

        resp = requests.post(url, headers=headers, files=files, data=data, timeout=60)

    print("Status:", resp.status_code)
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)

if __name__ == "__main__":
    run_ocr()
