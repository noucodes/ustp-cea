import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import json
import time
from typing import List, Dict, Any, Optional
import hashlib

# ---------------------
# Load environment variables
# ---------------------
load_dotenv(override=True)

# ---------------------
# Debug Mode & Config
# ---------------------
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.5"))  # 500ms default
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# ---------------------
# Logging Configuration
# ---------------------
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"scraper_log_{timestamp}.log"
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
if DEBUG_MODE:
    logger.debug("="*60)
    logger.debug("DEBUG MODE ENABLED - FULL TRACING ACTIVE")
    logger.debug("="*60)

# ---------------------
# Credentials & URLs
# ---------------------
USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")

if not USERNAME or not PASSWORD:
    logger.error("USERNAME1 or PASSWORD not found in .env file!")
    exit(1)

LOGIN_URL = "https://prisms.ustp.edu.ph/auth/login"
ENROLLMENT_URL = "https://prisms.ustp.edu.ph/enrollment/actions"
TRANSCRIPT_URL = "https://prisms.ustp.edu.ph/registrar/transcript/event"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# ---------------------
# Helper Functions
# ---------------------
def safe_bs_parse(html_content: str) -> BeautifulSoup:
    """Safely parse HTML content with BeautifulSoup"""
    return BeautifulSoup(html_content, "html.parser")

def extract_student_info(student: Dict[str, Any]) -> Dict[str, Any]:
    """Extract all student info in one pass - NO REPEATED PARSING"""
    try:
        col3 = student.get("3", "")
        col4 = student.get("4", "")
        col7 = student.get("7", "")
        col8 = student.get("8", "")
        col11 = student.get("11", "")
        
        id_soup = safe_bs_parse(str(col3))
        name_soup = safe_bs_parse(str(col4))
        course_soup = safe_bs_parse(str(col7))
        year_soup = safe_bs_parse(str(col8))
        link_soup = safe_bs_parse(str(col11))
        
        student_id = id_soup.get_text(strip=True)
        name = name_soup.get_text(strip=True)
        course = course_soup.get_text(strip=True)
        year_level_raw = year_soup.get_text(strip=True)
        
        # Extract encoded_id from link
        link_tag = link_soup.find("a")
        encoded_id = link_tag.get("data-idno") if link_tag else None
        
        # Smart year level detection
        year_level = categorize_year_level(year_level_raw)
        
        return {
            "student_id": student_id,
            "name": name,
            "course": course,
            "year_level_raw": year_level_raw,
            "year_level": year_level,
            "encoded_id": encoded_id
        }
    except Exception as e:
        logger.error(f"Failed to extract student info: {e}")
        return {}

def categorize_year_level(year_text: str) -> str:
    """Improved year level categorization"""
    if not year_text:
        return "Unknown"
    
    year_lower = year_text.lower().strip()
    
    if any(x in year_lower for x in ["1st", "first", "1 "]):
        return "1st Year"
    elif any(x in year_lower for x in ["2nd", "second", "2 "]):
        return "2nd Year"
    elif any(x in year_lower for x in ["3rd", "third", "3 "]):
        return "3rd Year"
    elif any(x in year_lower for x in ["4th", "fourth", "4 "]):
        return "4th Year"
    elif any(x in year_lower for x in ["5th", "fifth", "5 "]):
        return "5th Year"
    else:
        return "Unknown"

def save_json_incremental(data: List[Dict], filename: str, mode: str = 'a'):
    """Save JSON data incrementally (crash-safe)"""
    try:
        if mode == 'w' and os.path.exists(filename):
            os.remove(filename)
        
        with open(filename, 'a', encoding='utf-8') as f:
            # Write as newline-delimited JSON for incremental safety
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False, separators=(',', ':')) + '\n')
    except Exception as e:
        logger.error(f"Failed to save JSON incrementally: {e}")

def get_fresh_csrf(session: requests.Session, login_url: str) -> Optional[str]:
    """Get fresh CSRF token"""
    try:
        resp = session.get(login_url)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        token = soup.find("meta", {"name": "csrf-token"})
        return token["content"] if token else None
    except Exception as e:
        logger.error(f"Failed to get CSRF token: {e}")
        return None

# ---------------------
# Step 1: LOGIN
# ---------------------
def login() -> Optional[str]:
    logger.info("🔐 Starting login process...")
    csrf_token = get_fresh_csrf(session, LOGIN_URL)
    
    if not csrf_token:
        logger.error("❌ Failed to get CSRF token")
        return None
    
    if DEBUG_MODE:
        logger.debug(f"CSRF Token: {csrf_token[:20]}...")
    
    login_payload = {
        "_token": csrf_token,
        "Username": USERNAME,
        "password": PASSWORD,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(LOGIN_URL, data=login_payload, timeout=10)
            resp.raise_for_status()
            
            if "dashboard" in resp.url.lower() or resp.status_code == 200:
                logger.info("✅ Login successful!")
                return csrf_token
            else:
                logger.warning(f"Login attempt {attempt + 1} failed - unexpected redirect")
                
        except Exception as e:
            logger.warning(f"Login attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            time.sleep(1)
    
    logger.error("❌ Login failed after all retries")
    return None

# ---------------------
# Step 2: FETCH ENROLLMENT
# ---------------------
def fetch_enrollment(csrf_token: str) -> List[Dict]:
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
        "_": str(int(time.time() * 1000))
    }
    
    enrollment_headers = {
        "Referer": "https://prisms.ustp.edu.ph/enrollment/registration",
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }
    
    logger.info("📋 Fetching enrollment data...")
    try:
        resp = session.get(ENROLLMENT_URL, params=enrollment_params, headers=enrollment_headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        students = data.get("data", [])
        
        logger.info(f"✅ Retrieved {len(students)} students")
        
        # Save clean enrollment data immediately
        enrollment_data = [extract_student_info(student) for student in students]
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        enroll_filename = f"enrollment_students_{timestamp_str}.jsonl"
        save_json_incremental(enrollment_data, enroll_filename, 'w')
        logger.info(f"💾 Enrollment data saved: {enroll_filename}")
        
        if DEBUG_MODE:
            logger.debug(f"Sample student keys: {list(students[0].keys()) if students else 'No students'}")
        
        return students
    except Exception as e:
        logger.error(f"❌ Failed to fetch enrollment: {e}")
        return []

# ---------------------
# Step 3: MAIN PROCESSING ENGINE (FIXED INDENTATION!)
# ---------------------
def process_student_grades(students: List[Dict], csrf_token: str, students_by_year: Dict[str, List[Dict]]) -> List[Dict]:
    all_grades_data = []
    total_processed = 0
    
    for year_level, year_students in students_by_year.items():
        if not year_students:
            continue
            
        logger.info(f"🚀 Processing {year_level}: {len(year_students)} students")
        
        for idx, student in enumerate(year_students, 1):
            total_processed += 1
            student_info = extract_student_info(student)
            
            if not student_info.get("student_id"):
                logger.warning(f"⚠️ Skipping invalid student at index {idx}")
                continue
            
            student_id = student_info["student_id"]
            name = student_info["name"]
            course = student_info["course"]
            year_level_final = student_info["year_level"]
            encoded_id = student_info["encoded_id"]
            
            logger.info(f"[{total_processed}] Processing {year_level_final} #{idx}/{len(year_students)}: {student_id} - {name}")
            
            # Fetch grades for this student
            if encoded_id:
                grades = fetch_student_grades(encoded_id, csrf_token, student_id, DEBUG_MODE)
                if grades:
                    student_record = {
                        "student_id": student_id,
                        "name": name,
                        "course": course,
                        "year_level": year_level_final,
                        "encoded_id": encoded_id,
                        "total_subjects": len(grades),
                        "grades": grades
                    }
                    all_grades_data.append(student_record)
                    logger.info(f"✅ {student_id}: {len(grades)} grades retrieved")
                else:
                    logger.warning(f"⚠️ No grades for {student_id}")
            else:
                logger.warning(f"⚠️ No encoded ID for {student_id}")
            
            # Polite delay between requests
            if total_processed % 5 == 0:
                logger.info(f"⏳ Progress checkpoint: {total_processed}/{sum(len(s) for s in students_by_year.values())} (5 processed)")
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    return all_grades_data

def fetch_student_grades(encoded_id: str, csrf_token: str, student_id: str, debug: bool = False) -> List[Dict]:
    """Fetch grades for single student with retry logic"""
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
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.post(TRANSCRIPT_URL, data=payload, headers=headers, timeout=15)
            resp.raise_for_status()
            grade_data = resp.json()
            
            if grade_data.get("error"):
                logger.warning(f"Server error for {student_id}: {grade_data.get('message', 'Unknown')}")
                return []
            
            # Parse grades table
            html_content = grade_data.get("content", "")
            soup = BeautifulSoup(html_content, "html.parser")
            table = soup.find("table", {"id": "tblhistory"}) or soup.find("table")
            
            if not table:
                if debug:
                    logger.debug(f"No grades table found for {student_id}")
                return []
            
            grades = []
            print(f"\n  📚 {student_id} Grades:")
            print(f"  {'Code':<12} | {'Description':<40} | {'Grade':<8}")
            print(f"  {'-'*70}")
            
            for row in table.find_all("tr")[1:]:  # Skip header
                cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                if len(cols) >= 9 and cols[0] and cols[0] != "1.":  # Valid subject row
                    grades.append({
                        "subject_code": cols[1],
                        "subject_description": cols[2],
                        "grade": cols[7]
                    })
                    print(f"  {cols[1][:11]:<12} | {cols[2][:39]:<40} | {cols[7]:<8}")
            
            print()
            return grades
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {student_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
            continue
    
    logger.error(f"❌ All retries failed for {student_id}")
    return []

# ---------------------
# MAIN EXECUTION
# ---------------------
def main():
    logger.info("🚀 USTP Enrollment & Grades Scraper v2.0 - Starting...")
    
    # Step 1: Login
    csrf_token = login()
    if not csrf_token:
        return
    
    # Step 2: Fetch enrollment
    students = fetch_enrollment(csrf_token)
    if not students:
        logger.error("No students found. Exiting.")
        return
    
    # Step 3: Categorize by year
    students_by_year = {
        "1st Year": [], "2nd Year": [], "3rd Year": [], 
        "4th Year": [], "5th Year": [], "Unknown": []
    }
    
    for student in students:
        info = extract_student_info(student)
        if info.get("year_level"):
            students_by_year[info["year_level"]].append(student)
    
    logger.info(f"📊 Students by year: { {k: len(v) for k, v in students_by_year.items() if v} }")
    
    # Step 4: Process ALL students (FIXED!)
    all_grades_data = process_student_grades(students, csrf_token, students_by_year)
    
    # Step 5: Final summary & save
    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    grades_filename = f"grades_complete_{timestamp_str}.jsonl"
    
    print(f"\n{'='*120}")
    print(f"🎉 SUMMARY")
    print(f"{'='*120}")
    for year, count in students_by_year.items():
        if count:
            print(f"{year:<12}: {len(count):>4} students")
    print(f"📈 Total processed:        {len(students):>4}")
    print(f"📊 With grades retrieved:  {len(all_grades_data):>4}")
    print(f"💾 Data saved:             {grades_filename}")
    print(f"📝 Log file:               {log_filename}")
    
    if all_grades_data:
        save_json_incremental(all_grades_data, grades_filename, 'w')
        logger.info(f"✅ Complete grades data saved: {grades_filename} ({len(all_grades_data)} students)")
    
    logger.info("🎊 Script completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("👋 Script interrupted by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=DEBUG_MODE)