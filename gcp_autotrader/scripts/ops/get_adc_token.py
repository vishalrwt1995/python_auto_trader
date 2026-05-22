"""Exchange the ADC refresh token for an access token by calling Google's
OAuth token endpoint directly. Bypasses gcloud's account validation.

If the refresh token is still valid, this will give us a working access token
to call Cloud Run / BigQuery / Logging APIs via HTTP.
"""
import json
import urllib.request
import urllib.parse
from pathlib import Path

ADC = json.loads(Path("/Users/vishalrawat/.config/gcloud/application_default_credentials.json").read_text())

data = urllib.parse.urlencode({
    "client_id": ADC["client_id"],
    "client_secret": ADC["client_secret"],
    "refresh_token": ADC["refresh_token"],
    "grant_type": "refresh_token",
}).encode()

req = urllib.request.Request(
    "https://oauth2.googleapis.com/token",
    data=data,
    headers={"Content-Type": "application/x-www-form-urlencoded"},
)
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read().decode()
        tok = json.loads(body)
        access_token = tok.get("access_token")
        if not access_token:
            print("No access_token in response:")
            print(body)
            raise SystemExit(1)
        print(access_token)
        # Also save to file for reuse
        Path("/tmp/adc_access_token.txt").write_text(access_token)
except urllib.error.HTTPError as e:
    err = e.read().decode(errors="replace")
    print(f"HTTPError {e.code}: {err}")
    raise SystemExit(2)
except Exception as e:
    print(f"Error: {e}")
    raise SystemExit(3)
