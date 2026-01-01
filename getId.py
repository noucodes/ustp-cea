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
ENROLLMENT_URL = "https://prisms.ustp.edu.ph/enrollment/actions"

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
# Step 7: Fetch Enrollment Data (GET Request)
# ---------------------

# Cpe - cTFEaXV4RTUrQjkrT1VhMnp6aWNXQT09
# CE - eUd1UTFQVzZpbGtUTjFQUXl2T1dvdz09

# Extracting the key parameters from your provided URL
enrollment_params = {
    "event": "registered",
    "level": "-1",
    "term": "187",
    "campus": "1",
    "progid": "cTFEaXV4RTUrQjkrT1VhMnp6aWNXQT09",
    "validation_status": "0",
    "section": "",
    "draw": "40",
    "start": "0",
    "length": "-1", # -1 usually means "Fetch All Records"
    "order[0][column]": "2",
    "order[0][dir]": "desc",
    "search[value]": "",
    "search[regex]": "false",
    "_": "1767077522099" # Timestamp/Cache-buster
}

enrollment_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
    "Referer": "https://prisms.ustp.edu.ph/enrollment/registration",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

print("Fetching registered students list...")
# We use 'params' here instead of 'data' because it is a GET request
enroll_resp = session.get(ENROLLMENT_URL, params=enrollment_params, headers=enrollment_headers)
enroll_resp.raise_for_status()

# # ---------------------
# # Step 8: Find Specific Student ID
# # ---------------------

# # Your target ID
# target_id = "2023308388"

# # Assuming 'enroll_resp' is your JSON response from the session.get() call
# data = enroll_resp.json()
# students = data.get("data", [])

# found = False

# for student in students:
#     # 1. Extract the Student ID from index 3
#     # It looks like: <span class="autofit reg-id" ...>2023308388</span>
#     id_html = student.get("3", "")
#     id_soup = BeautifulSoup(id_html, "html.parser")
#     current_student_id = id_soup.get_text().strip()

#     # 2. Check if this is the student we are looking for
#     if current_student_id == target_id:
#         found = True
        
#         # 3. Extract the Name from index 4 for verification
#         name_html = student.get("4", "")
#         name = BeautifulSoup(name_html, "html.parser").get_text().strip()

#         # 4. Extract the Encoded ID from index 11
#         # It's inside: <a href="..." data-idno="ENCODED_VALUE_HERE" ...>
#         link_html = student.get("11", "")
#         link_soup = BeautifulSoup(link_html, "html.parser")
#         link_tag = link_soup.find("a")
        
#         encoded_id = link_tag.get("data-idno") if link_tag else "Not Found"

#         print(f"--- MATCH FOUND ---")
#         print(f"Student ID : {current_student_id}")
#         print(f"Name       : {name}")
#         print(f"Encoded ID : {encoded_id}")
#         print(f"-------------------")
#         break 

# if not found:
#     print(f"ID {target_id} was not found in the current enrollment list.")

# ---------------------
# Step 8: Parse Enrollment List
# ---------------------
data = enroll_resp.json()
students = data.get("data", [])

print(f"{'ID NO':<15} | {'NAME':<25} | {'COURSE':<10} | {'ENCODED ID'}")
print("-" * 100)

for student in students:
    # 1. Extract Student ID (Column index 3 has it inside a span)
    id_soup = BeautifulSoup(student.get("3", ""), "html.parser")
    student_id = id_soup.get_text().strip()

    # 2. Extract Name (Column index 4)
    name_soup = BeautifulSoup(student.get("4", ""), "html.parser")
    name = name_soup.get_text().strip()

    # 3. Extract Course (Column index 7)
    course_soup = BeautifulSoup(student.get("7", ""), "html.parser")
    course = course_soup.get_text().strip()

    # 4. Extract THE ENCODED ID (Column index 11 contains the data-idno)
    # This is usually what you need for the transcript URL!
    link_soup = BeautifulSoup(student.get("11", ""), "html.parser")
    link_tag = link_soup.find("a")
    encoded_id = link_tag.get("data-idno") if link_tag else "N/A"

    print(f"{student_id:<15} | {name:<25} | {course:<10} | {encoded_id}")