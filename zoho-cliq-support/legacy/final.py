import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
ACCESS_TOKEN = os.getenv("CATALYST_TOKEN")

def run_ocr():
    if not ACCESS_TOKEN:
        raise RuntimeError("Set CATALYST_TOKEN")

    url = f"https://api.catalyst.zoho.in/baas/v1/project/{PROJECT_ID}/ml/ocr"

    headers = {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
    }

    with open("sample.png", "rb") as f:
        files = {"image": ("sample.png", f, "image/png")}
        data = {"language": "eng"}
        resp = requests.post(url, headers=headers, files=files, data=data, timeout=60)

    print("Status:", resp.status_code)
    print(json.dumps(resp.json(), indent=2))

if __name__ == "__main__":
    run_ocr()
