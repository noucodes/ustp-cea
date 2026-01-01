import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import json

def extract_students_to_json(students, output_prefix="enrollment_students"):
    extracted = []

    for student in students:
        try:
            # Student ID (col 3)
            student_id = BeautifulSoup(
                student.get("3", ""), "html.parser"
            ).get_text(strip=True)

            # Name (col 4)
            name = BeautifulSoup(
                student.get("4", ""), "html.parser"
            ).get_text(strip=True)

            # Course (col 7)
            course = BeautifulSoup(
                student.get("7", ""), "html.parser"
            ).get_text(strip=True)

            # Year Level (col 8)
            year_level = BeautifulSoup(
                str(student.get("8", "")), "html.parser"
            ).get_text(strip=True) or "Unknown"

            # Encoded ID (col 11)
            link_soup = BeautifulSoup(student.get("11", ""), "html.parser")
            link_tag = link_soup.find("a")
            encoded_id = link_tag.get("data-idno") if link_tag else None

            extracted.append({
                "student_id": student_id,
                "name": name,
                "course": course,
                "year_level": year_level,
                "encoded_id": encoded_id
            })

        except Exception as e:
            logger.error(f"Failed to extract student row: {e}")

    filename = f"{output_prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"Extracted {len(extracted)} students → {filename}")
    return extracted

# ---------------------
# Load environment variables
# ---------------------
load_dotenv(override=True)

# ---------------------
# Debug Mode
# ---------------------
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"  # Set to True to enable debug mode

# ---------------------
# Logging Configuration
# ---------------------
log_filename = f"scraper_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

if DEBUG_MODE:
    logger.debug("="*50)
    logger.debug("DEBUG MODE ENABLED")
    logger.debug("="*50)

# ---------------------
# Configuration
# ---------------------
USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")

if DEBUG_MODE:
    logger.debug(f"Username loaded: {'Yes' if USERNAME else 'No'}")
    logger.debug(f"Password loaded: {'Yes' if PASSWORD else 'No'}")

LOGIN_URL = "https://prisms.ustp.edu.ph/auth/login"
ENROLLMENT_URL = "https://prisms.ustp.edu.ph/enrollment/actions"
TRANSCRIPT_URL = "https://prisms.ustp.edu.ph/registrar/transcript/event"

if DEBUG_MODE:
    logger.debug(f"LOGIN_URL: {LOGIN_URL}")
    logger.debug(f"ENROLLMENT_URL: {ENROLLMENT_URL}")
    logger.debug(f"TRANSCRIPT_URL: {TRANSCRIPT_URL}")

session = requests.Session()

# ---------------------
# Step 1: Login & Get CSRF
# ---------------------
logger.info("Starting login process...")
try:
    get_resp = session.get(LOGIN_URL)
    get_resp.raise_for_status()
    
    if DEBUG_MODE:
        logger.debug(f"Login page status code: {get_resp.status_code}")
        logger.debug(f"Login page response length: {len(get_resp.text)} chars")
    
    soup = BeautifulSoup(get_resp.text, "html.parser")
    csrf_token = soup.find("meta", {"name": "csrf-token"})["content"]
    logger.info("CSRF token retrieved successfully")
    
    if DEBUG_MODE:
        logger.debug(f"CSRF Token: {csrf_token[:20]}...")

    login_payload = {
        "_token": csrf_token,
        "Username": USERNAME,
        "password": PASSWORD,
    }

    if DEBUG_MODE:
        logger.debug(f"Login payload keys: {list(login_payload.keys())}")

    login_resp = session.post(LOGIN_URL, data=login_payload)
    login_resp.raise_for_status()
    
    if DEBUG_MODE:
        logger.debug(f"Login response status: {login_resp.status_code}")
        logger.debug(f"Login response URL: {login_resp.url}")
    
    logger.info("Login successful")
except Exception as e:
    logger.error(f"Login failed: {str(e)}")
    raise

# ---------------------
# Step 2: Fetch Enrollment Data
# ---------------------
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
    "length": "-1",
    "order[0][column]": "2",
    "order[0][dir]": "desc",
    "search[value]": "",
    "search[regex]": "false",
    "_": "1767077522099"
}

enrollment_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36...",
    "Referer": "https://prisms.ustp.edu.ph/enrollment/registration",
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

logger.info("Fetching registered students list...")
try:
    if DEBUG_MODE:
        logger.debug(f"Enrollment params: {enrollment_params}")
        
    enroll_resp = session.get(ENROLLMENT_URL, params=enrollment_params, headers=enrollment_headers)
    enroll_resp.raise_for_status()
    
    if DEBUG_MODE:
        logger.debug(f"Enrollment response status: {enroll_resp.status_code}")
        logger.debug(f"Enrollment response length: {len(enroll_resp.text)} chars")
    
    data = enroll_resp.json()
    students = data.get("data", [])
    logger.info(f"Successfully retrieved {len(students)} students")
    # ---------------------
    # Step 2.5: Extract Clean Enrollment List to JSON
    # ---------------------
    clean_students = extract_students_to_json(students)
    
    if DEBUG_MODE and students:
        logger.debug(f"Sample student data keys: {list(students[0].keys())}")
        logger.debug(f"First student raw data: {students[0]}")
        
except Exception as e:
    logger.error(f"Failed to fetch enrollment data: {str(e)}")
    raise

# ---------------------
# Step 3: Parse Enrollment List & Fetch Grades (Separated by Year Level)
# ---------------------

# Organize students by year level
students_by_year = {
    "1st Year": [],
    "2nd Year": [],
    "3rd Year": [],
    "4th Year": [],
    "5th Year": [],
    "Unknown": []
}

# First pass: categorize students by year level
for student in students:
    try:
        # Extract year level from column index 8 (changed from 7)
        year_data = student.get("8", "")
        
        # Convert to string if it's a number or other type
        if not isinstance(year_data, str):
            year_data = str(year_data)
        
        year_soup = BeautifulSoup(year_data, "html.parser")
        year_level = year_soup.get_text().strip()
        
        if DEBUG_MODE:
            logger.debug(f"Raw year level data type: {type(student.get('7'))}, value: '{year_level}'")
        
        # Map year level to category based on format like "3rd Year - Baccalaureate"
        if "1st" in year_level or "First" in year_level or year_level.startswith("1"):
            students_by_year["1st Year"].append(student)
        elif "2nd" in year_level or "Second" in year_level or year_level.startswith("2"):
            students_by_year["2nd Year"].append(student)
        elif "3rd" in year_level or "Third" in year_level or year_level.startswith("3"):
            students_by_year["3rd Year"].append(student)
        elif "4th" in year_level or "Fourth" in year_level or year_level.startswith("4"):
            students_by_year["4th Year"].append(student)
        elif "5th" in year_level or "Fifth" in year_level or year_level.startswith("5"):
            students_by_year["5th Year"].append(student)
        else:
            students_by_year["Unknown"].append(student)
            if DEBUG_MODE:
                logger.debug(f"Unknown year level: '{year_level}'")
    except Exception as e:
        logger.error(f"Error categorizing student: {str(e)}")
        if DEBUG_MODE:
            logger.debug(f"Problematic student data column 7: {student.get('7')} (type: {type(student.get('7'))})")
        students_by_year["Unknown"].append(student)

logger.debug(f"All students categorized by year level. {students_by_year}")
all_grades_data = []
total_processed = 0

# Process each year level separately
for year_level, year_students in students_by_year.items():
    if not year_students:
        continue
        
    print(f"\n{'='*120}")
    print(f"{year_level.upper()} - {len(year_students)} STUDENTS")
    print(f"{'='*120}")
    print(f"{'ID NO':<15} | {'NAME':<25} | {'COURSE':<10} | {'ENCODED ID':<50}")
    print(f"{'-'*120}\n")
    
    logger.info(f"Processing {year_level}: {len(year_students)} students")

    for idx, student in enumerate(year_students, 1):
        total_processed += 1
        try:
            # Extract Student Info
            id_soup = BeautifulSoup(student.get("3", ""), "html.parser")
            student_id = id_soup.get_text().strip()

            name_soup = BeautifulSoup(student.get("4", ""), "html.parser")
            name = name_soup.get_text().strip()

            course_soup = BeautifulSoup(student.get("7", ""), "html.parser")
            course = course_soup.get_text().strip()

            link_soup = BeautifulSoup(student.get("11", ""), "html.parser")
            link_tag = link_soup.find("a")
            encoded_id = link_tag.get("data-idno") if link_tag else None

            if DEBUG_MODE:
                logger.debug(f"Student {student_id}: name='{name}', course='{course}', encoded_id='{encoded_id}'")

            print(f"{student_id:<15} | {name:<25} | {course:<10} | {encoded_id or 'N/A':<50}")
            logger.info(f"Processing {year_level} student {idx}/{len(year_students)}: {student_id} - {name}")

            # ---------------------
            # Step 4: Fetch Grades for This Student
            # ---------------------
            if encoded_id:
                try:
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

                    if DEBUG_MODE:
                        logger.debug(f"Fetching grades for {student_id} with encoded_id: {encoded_id}")

                    resp = session.post(TRANSCRIPT_URL, data=payload, headers=headers)
                    resp.raise_for_status()
                    grade_data = resp.json()

                    if DEBUG_MODE:
                        logger.debug(f"Grade response status: {resp.status_code}")
                        logger.debug(f"Grade response has error: {grade_data.get('error', False)}")

                    if not grade_data.get("error"):
                        html_string = grade_data.get("content", "")
                        grade_soup = BeautifulSoup(html_string, "html.parser")
                        table = grade_soup.find("table", {"id": "tblhistory"}) or grade_soup.find("table")
                        
                        student_grades = []
                        
                        if table:
                            if DEBUG_MODE:
                                logger.debug(f"Found grades table for {student_id}")
                                
                            print(f"\n  {'Subject Code':<15} | {'Subject Description':<45} | {'Grade'}")
                            print(f"  {'-'*80}")
                            
                            for row in table.find_all("tr"):
                                cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                                
                                if DEBUG_MODE and len(cols) > 0:
                                    logger.debug(f"Row has {len(cols)} columns: {cols[:3]}...")
                                
                                if len(cols) > 8 and cols[0] != "" and cols[0] != "1." and cols[1]:
                                    subject_code = cols[1]
                                    subject_desc = cols[2]
                                    grade = cols[8]
                                    
                                    print(f"  {subject_code:<15} | {subject_desc:<45} | {grade}")
                                    student_grades.append({
                                        "subject_code": subject_code,
                                        "subject_description": subject_desc,
                                        "grade": grade
                                    })
                            
                            logger.info(f"Retrieved {len(student_grades)} grades for {student_id}")
                            print()  # Add spacing after grades
                        else:
                            logger.warning(f"No grades table found for {student_id}")
                        
                        all_grades_data.append({
                            "student_id": student_id,
                            "name": name,
                            "course": course,
                            "year_level": year_level,
                            "encoded_id": encoded_id,
                            "program": grade_data.get("program"),
                            "grades": student_grades
                        })
                    else:
                        logger.warning(f"Server returned error for student {student_id}")
                        
                except Exception as e:
                    logger.error(f"Failed to fetch grades for {student_id}: {str(e)}")
            else:
                logger.warning(f"No encoded ID found for student {student_id}")
                
        except Exception as e:
            logger.error(f"Error processing student at index {idx}: {str(e)}")

# ---------------------
# Summary
# ---------------------
print(f"\n{'='*120}")
print(f"SUMMARY")
print(f"{'='*120}")
for year_level, year_students in students_by_year.items():
    if year_students:
        print(f"{year_level}: {len(year_students)} students")
print(f"Total students processed: {total_processed}")
print(f"Students with grades retrieved: {len(all_grades_data)}")
logger.info(f"Script completed. Total students: {total_processed}, With grades: {len(all_grades_data)}")
logger.info(f"Log file saved as: {log_filename}")

# Optional: Save all grades data to a file
output_filename = f"grades_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(output_filename, 'w') as f:
    json.dump(all_grades_data, f, indent=2)
logger.info(f"Grades data saved to: {output_filename}")