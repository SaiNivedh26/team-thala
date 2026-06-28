import os
import requests
from dotenv import load_dotenv

load_dotenv()

BUCKET_URL = os.getenv("BUCKET_URL")
ACCESS_TOKEN = os.getenv("CATALYST_TOKEN")  # Your Zoho OAuth token

def upload_to_stratus():
    if not ACCESS_TOKEN:
        raise RuntimeError("Set CATALYST_TOKEN env var")

    # File to upload
    file_path = "sample.png"
    
    if not os.path.exists(file_path):
        raise RuntimeError(f"{file_path} not found")

    # Open file in binary mode
    with open(file_path, "rb") as f:
        file_content = f.read()

    # Stratus upload endpoint
    # The path in the bucket where you want to upload the file
    object_key = "sample.png"  # or any path like "images/sample.png"
    
    url = f"{BUCKET_URL}/{object_key}"

    headers = {
        "Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
        "compress": "false",  # Set to "true" if you want compression
        "cache-control": "max-age=3600",  # Optional: cache for 1 hour
        # "expires-after": "3600",  # Optional: TTL in seconds
        # "x-user-meta": "key1=value1;key2=value2",  # Optional: metadata
    }

    resp = requests.put(url, data=file_content, headers=headers, timeout=60)

    print("Status:", resp.status_code)
    print("Response:", resp.text)

    if resp.status_code in [200, 201, 204]:
        print(f"✓ Successfully uploaded {file_path} to Stratus")
        print(f"  Bucket URL: {url}")
    else:
        print(f"✗ Upload failed")
        try:
            import json
            print(json.dumps(resp.json(), indent=2))
        except:
            pass

if __name__ == "__main__":
    upload_to_stratus()
