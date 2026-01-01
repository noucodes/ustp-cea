import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

load_dotenv()

USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
LOGIN_URL = os.getenv("LOGIN_URL")

session = requests.Session()

# GET login page
resp = session.get(LOGIN_URL)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "html.parser")

csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]
print("CSRF token:", csrf_token)

# Build payload
payload = {
    "_token": csrf_token,
    "username": USERNAME,
    "password": PASSWORD,
}

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": LOGIN_URL,
    "Origin": "https://prisms.ustp.edu.ph",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-CSRF-TOKEN": csrf_token
}

# POST login
login_resp = session.post(LOGIN_URL, data=payload, headers=headers, allow_redirects=False)

print("Status code:", login_resp.status_code)
print("Location header:", login_resp.headers.get("Location"))
print("Cookies:", session.cookies.get_dict())
