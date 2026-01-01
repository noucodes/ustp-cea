import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import os

load_dotenv()

# ---------------------
# Environment variables
# ---------------------
USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")
LOGIN_URL = os.getenv("LOGIN_URL")
DATATABLE_URL = os.getenv("DATATABLE_URL")

# ---------------------
# Create session
# ---------------------
session = requests.Session()

# ---------------------
# Step 1: GET login page
# ---------------------
resp = session.get(LOGIN_URL)
resp.raise_for_status()
soup = BeautifulSoup(resp.text, "html.parser")

# Extract CSRF token from <meta> tag
csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]
print("CSRF token:", csrf_token)

# ---------------------
# Step 2: POST login
# ---------------------
login_payload = {
    "_token": csrf_token,
    "Username": USERNAME,
    "password": PASSWORD,
}

login_headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": LOGIN_URL,
    "Origin": "https://prisms.ustp.edu.ph",
    "Content-Type": "application/x-www-form-urlencoded",
    "X-CSRF-TOKEN": csrf_token,
}

login_resp = session.post(LOGIN_URL, data=login_payload, headers=login_headers, allow_redirects=True)
login_resp.raise_for_status()
print("Login successful. Status code:", login_resp.status_code)

# ---------------------
# Step 3: POST DataTable request
# ---------------------
datatable_payload = {
    "draw": 2,
    "columns[0][data]": 0,
    "columns[0][name]": "",
    "columns[0][searchable]": "true",
    "columns[0][orderable]": "true",
    "columns[0][search][value]": "",
    "columns[0][search][regex]": "false",
    "columns[1][data]": 1,
    "columns[1][name]": "",
    "columns[1][searchable]": "true",
    "columns[1][orderable]": "true",
    "columns[1][search][value]": "",
    "columns[1][search][regex]": "false",
    "columns[2][data]": 2,
    "columns[2][name]": "",
    "columns[2][searchable]": "true",
    "columns[2][orderable]": "true",
    "columns[2][search][value]": "",
    "columns[2][search][regex]": "false",
    "columns[3][data]": 3,
    "columns[3][name]": "",
    "columns[3][searchable]": "true",
    "columns[3][orderable]": "true",
    "columns[3][search][value]": "",
    "columns[3][search][regex]": "false",
    "columns[4][data]": 4,
    "columns[4][name]": "",
    "columns[4][searchable]": "true",
    "columns[4][orderable]": "true",
    "columns[4][search][value]": "",
    "columns[4][search][regex]": "false",
    "columns[5][data]": 5,
    "columns[5][name]": "",
    "columns[5][searchable]": "true",
    "columns[5][orderable]": "true",
    "columns[5][search][value]": "",
    "columns[5][search][regex]": "false",
    "columns[6][data]": 6,
    "columns[6][name]": "",
    "columns[6][searchable]": "true",
    "columns[6][orderable]": "true",
    "columns[6][search][value]": "",
    "columns[6][search][regex]": "false",
    "columns[7][data]": 7,
    "columns[7][name]": "",
    "columns[7][searchable]": "true",
    "columns[7][orderable]": "true",
    "columns[7][search][value]": "",
    "columns[7][search][regex]": "false",
    "order[0][column]": 0,
    "order[0][dir]": "asc",
    "start": 0,
    "length": 10,
    "search[value]": "2023308388",  # Example search
    "search[regex]": "false",
    "event": "get",
    "returnType": 0,
    "campusid": 0,
}   

datatable_headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://prisms.ustp.edu.ph/registrar/transcript",
    "Origin": "https://prisms.ustp.edu.ph",
    "X-CSRF-TOKEN": csrf_token,  # <-- this is the important fix
    "X-Requested-With": "XMLHttpRequest",
    "Content-Type": "application/x-www-form-urlencoded",
}

resp = session.post(DATATABLE_URL, data=datatable_payload, headers=datatable_headers)
resp.raise_for_status()

# ---------------------
# Step 4: Parse JSON response
# ---------------------
data = resp.json()

# 'data' is a list of dictionaries in this specific response
students = data.get("data", [])

if students:
    print(f"Found {len(students)} records. Showing first 10:\n")
    print(f"{'ID Number':<15} | {'Student Name':<30} | {'Encoded ID'}")
    print("-" * 80)

    for student in students:
        # Extracting from the top-level keys you showed
        id_number = student.get("1")  # Index "1" holds "2-2010100604"
        
        # Extracting name from the HTML in index "2"
        raw_name_html = student.get("2", "")
        name_soup = BeautifulSoup(raw_name_html, "html.parser")
        student_name = name_soup.get_text().strip()
        
        # Extracting the target from DT_RowData
        row_data = student.get("DT_RowData", {})
        encoded_id = row_data.get("studentno_encoded")

        print(f"{id_number:<15} | {student_name:<30} | {encoded_id}")
else:
    print("No data found in the response.")