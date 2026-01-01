import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv

load_dotenv(override=True)

# ---------------------
# Configuration
# ---------------------
USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")
LOGIN_URL = "https://prisms.ustp.edu.ph/auth/login"
TRANSCRIPT_URL = "https://prisms.ustp.edu.ph/registrar/transcript/event"

session = requests.Session()

# ---------------------
# Step 1: Login & Get CSRF
# ---------------------
print("Logging in...")
get_resp = session.get(LOGIN_URL)
soup = BeautifulSoup(get_resp.text, "html.parser")
csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]

login_payload = {
    "_token": csrf_token,
    "Username": USERNAME, # Note: Use 'Username' or 'email' based on your previous test
    "password": PASSWORD,
}

login_resp = session.post(LOGIN_URL, data=login_payload)
login_resp.raise_for_status()

# ---------------------
# Step 2: Fetch Grades via POST
# ---------------------
# This is the encoded ID you extracted earlier
encoded_id = "eW84ZmFUMnhvMGRhVWxtU1U0cHVCYkJQTS9uMG9SM01ibUdzYXV6ZlhrST0-"

payload = {
    "_token": csrf_token,
    "event": "load-grades",
    "progClass": "50",
    "idno": encoded_id
}

headers = {
    "X-CSRF-TOKEN": csrf_token,
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"https://prisms.ustp.edu.ph/registrar/transcript/student?idno={encoded_id}",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
}

print(f"Fetching grades for student ID: {encoded_id}...")
resp = session.post(TRANSCRIPT_URL, data=payload, headers=headers)
resp.raise_for_status()

# ---------------------
# Step 3: Parse the JSON Response
# ---------------------
data = resp.json()

if not data.get("error"):
    print(f"\nPROGRAM: {data.get('program')}")
    print(f"CLASS ID: {data.get('progClass')}")
    
    # Extract the HTML string from the 'content' key
    html_string = data.get("content", "")
    grade_soup = BeautifulSoup(html_string, "html.parser")
    
    # Look for the table (id 'tblhistory' based on your JS snippet)
    table = grade_soup.find("table", {"id": "tblhistory"}) or grade_soup.find("table")
    
    if table:
        print("\n" + "="*80)
        print(f"{'Subject Code':<15} | {'Subject Description':<45} | {'Grade'}")
        print("="*80)
        
        # Iterate through rows, skipping the header if necessary
        for row in table.find_all("tr"):
            cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
            
            # Filter for rows that actually contain data (usually 3+ columns)
            if len(cols) > 3 and cols[0] != "" and cols[0] != "1.":

                # Subject Code | Subject Description | Final Equivalent Grade
                # Adjust index numbers [1], [2], [8] based on actual table layout
                print(f"{cols[1]:<15} | {cols[2]:<45} | {cols[8]}")
    else:
        print("Could not find the grades table in the response content.")
else:
    print("Server returned an error for this student ID.")