import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

# ---------------------
# Load environment variables
# ---------------------
load_dotenv(override=True)

# ---------------------
# Config
# ---------------------
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))

USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")
PROG_ID = os.getenv("PROGID")

if not USERNAME or not PASSWORD or not PROG_ID:
    print("ERROR: Missing USERNAME1, PASSWORD, or PROGID in .env file!")
    exit(1)

# ---------------------
# Logging Setup
# ---------------------
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"scraper_and_process_{timestamp}.log"

logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------
# Requests Session
# ---------------------
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

LOGIN_URL = "https://prisms.ustp.edu.ph/auth/login"
ENROLLMENT_URL = "https://prisms.ustp.edu.ph/enrollment/actions"
TRANSCRIPT_URL = "https://prisms.ustp.edu.ph/registrar/transcript/event"

# ---------------------
# Helper Functions
# ---------------------
def safe_bs_parse(html_content: str) -> BeautifulSoup:
    return BeautifulSoup(html_content, "html.parser")

def extract_student_info(student: Dict[str, Any]) -> Dict[str, Any]:
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
        link_tag = link_soup.find("a")
        encoded_id = link_tag.get("data-idno") if link_tag else None
        
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
    if not year_text:
        return "Unknown"
    year_lower = year_text.lower().strip()
    if any(x in year_lower for x in ["1st", "first", "1 "]): return "1st Year"
    elif any(x in year_lower for x in ["2nd", "second", "2 "]): return "2nd Year"
    elif any(x in year_lower for x in ["3rd", "third", "3 "]): return "3rd Year"
    elif any(x in year_lower for x in ["4th", "fourth", "4 "]): return "4th Year"
    elif any(x in year_lower for x in ["5th", "fifth", "5 "]): return "5th Year"
    else: return "Unknown"

def save_jsonl(data: List[Dict], filename: str, mode: str = 'w'):
    try:
        with open(filename, mode, encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")

def get_fresh_csrf(sess: requests.Session) -> Optional[str]:
    try:
        resp = sess.get(LOGIN_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        token = soup.find("meta", {"name": "csrf-token"})
        return token["content"] if token else None
    except Exception as e:
        logger.error(f"CSRF fetch failed: {e}")
        return None

# ---------------------
# Login
# ---------------------
def login() -> bool:
    logger.info("Logging in...")
    csrf = get_fresh_csrf(session)
    if not csrf:
        logger.error("No CSRF token")
        return False

    payload = {"_token": csrf, "Username": USERNAME, "password": PASSWORD}
    try:
        resp = session.post(LOGIN_URL, data=payload, timeout=15)
        if "dashboard" in resp.url or resp.status_code == 200:
            logger.info("Login successful!")
            return True
    except Exception as e:
        logger.error(f"Login failed: {e}")
    return False

# ---------------------
# Fetch Enrollment
# ---------------------
def fetch_enrollment() -> List[Dict]:
    params = {
        "event": "registered", "level": "-1", "term": "187", "campus": "1",
        "progid": PROG_ID, "validation_status": "0", "section": "", "draw": "1",
        "start": "0", "length": "-1", "_": str(int(time.time() * 1000))
    }
    headers = {"X-CSRF-TOKEN": get_fresh_csrf(session), "X-Requested-With": "XMLHttpRequest"}

    try:
        resp = session.get(ENROLLMENT_URL, params=params, headers=headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        students = data.get("data", [])
        logger.info(f"Fetched {len(students)} enrolled students")
        return students
    except Exception as e:
        logger.error(f"Enrollment fetch failed: {e}")
        return []

# ---------------------
# Fetch Grades (Worker)
# ---------------------
def fetch_student_grades(encoded_id: str) -> List[Dict]:
    worker_session = requests.Session()
    worker_session.headers.update(session.headers)
    
    csrf = get_fresh_csrf(worker_session)
    if not csrf:
        return []

    # Login worker
    worker_session.post(LOGIN_URL, data={"_token": csrf, "Username": USERNAME, "password": PASSWORD})

    payload = {"_token": csrf, "event": "load-grades", "progClass": "50", "idno": encoded_id}
    headers = {"X-CSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest"}

    try:
        resp = worker_session.post(TRANSCRIPT_URL, data=payload, headers=headers, timeout=15)
        resp.raise_for_status()
        content = resp.json().get("content", "")
        soup = BeautifulSoup(content, "html.parser")
        table = soup.find("table", {"id": "tblhistory"}) or soup.find("table")

        grades = []
        if table:
            for row in table.find_all("tr")[1:]:
                cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                if len(cols) >= 9 and cols[0] and cols[0] != "1.":
                    grades.append({
                        "subject_code": cols[1],
                        "subject_description": cols[2],
                        "subject_unit": cols[3],
                        "grade": cols[7]
                    })
        return grades
    except Exception as e:
        logger.warning(f"Grades fetch failed for {encoded_id}: {e}")
        return []

# ---------------------
# POST-PROCESSING: Fix Duplicates + Classify Status
# ---------------------
def is_irregular_grade(grade_val: Any) -> bool:
    g = str(grade_val).strip()
    return g in {"", "INC", "5", "5.0", "5.00", "W", "D/F"}

def classify_status(grades: List[Dict]) -> Tuple[str, List[str]]:
    reasons = []
    for i, g in enumerate(grades):
        if is_irregular_grade(g.get("grade")):
            reasons.append(f"index {i}: '{g.get('grade')}'")
    return ("Irregular" if reasons else "Regular"), reasons

def fix_duplicates_and_classify(student: Dict) -> Dict:
    grades = student.get("grades", [])
    student_id = student.get("student_id", "Unknown")
    name = student.get("name", "No name")

    # Fix duplicates: keep last occurrence
    seen = defaultdict(list)
    for i, g in enumerate(grades):
        code = g.get("subject_code", "").strip().upper()
        if code:
            seen[code].append(i)

    to_remove = set()
    for code, indices in seen.items():
        if len(indices) > 1:
            keep = indices[-1]
            for idx in indices[:-1]:
                to_remove.add(idx)
            logger.info(f"Duplicate {code} in {student_id} → keeping last, removing {len(indices)-1} earlier")

    if to_remove:
        student["grades"] = [g for i, g in enumerate(grades) if i not in to_remove]

    # Classify status
    status, reasons = classify_status(student["grades"])
    student["enrollment_status"] = status

    if status == "Irregular":
        logger.warning(f"IRREGULAR: {student_id} - {name} | Reasons: {'; '.join(reasons)}")
    else:
        logger.info(f"REGULAR: {student_id} - {name}")

    return student

# ---------------------
# Main Execution
# ---------------------
def main():
    logger.info("USTP Grades Scraper + Processor v3.0 - Starting...")

    if not login():
        return

    students_raw = fetch_enrollment()
    if not students_raw:
        logger.error("No students fetched. Exiting.")
        return

    # Categorize by year
    students_by_year = defaultdict(list)
    for s in students_raw:
        info = extract_student_info(s)
        if info.get("encoded_id"):
            students_by_year[info["year_level"]].append(info)

    logger.info(f"Students by year: {dict((k, len(v)) for k, v in students_by_year.items() if v)}")

    # Scrape grades concurrently
    raw_records = []
    total = sum(len(v) for v in students_by_year.values())
    processed = 0

    tasks = []
    for year, infos in students_by_year.items():
        for info in infos:
            tasks.append((info, year))

    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    raw_filename = f"grades_complete_{timestamp_str}.jsonl"
    final_filename = f"grades_final_with_status_{timestamp_str}.jsonl"

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_student_grades, info["encoded_id"]): info for info, _ in tasks}

        for future in as_completed(futures):
            info = futures[future]
            processed += 1
            grades = future.result()

            record = {
                "student_id": info["student_id"],
                "name": info["name"],
                "course": info["course"],
                "year_level": info["year_level"],
                "total_subjects": len(grades),
                "grades": grades
            }
            raw_records.append(record)
            save_jsonl([record], raw_filename, 'a')

            logger.info(f"[{processed}/{total}] Fetched grades: {info['student_id']} - {len(grades)} subjects")

            time.sleep(DELAY_BETWEEN_REQUESTS)

    # Save raw complete data
    save_jsonl(raw_records, raw_filename, 'w')
    logger.info(f"Raw grades saved: {raw_filename}")

    # Process: fix duplicates + classify status
    logger.info("Starting post-processing: fixing duplicates + classifying status...")
    final_records = [fix_duplicates_and_classify(r) for r in raw_records]

    # Save final enriched data
    save_jsonl(final_records, final_filename, 'w')

    # Final summary
    regular = sum(1 for r in final_records if r.get("enrollment_status") == "Regular")
    irregular = len(final_records) - regular

    print("\n" + "="*80)
    print("SCRAPING & PROCESSING COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Raw data:           {raw_filename}")
    print(f"Final data:         {final_filename}")
    print(f"Total students:     {len(final_records)}")
    print(f"   Regular:         {regular}")
    print(f"   Irregular:       {irregular}")
    print(f"Log file:           {log_filename}")
    print("="*80)

    logger.info("All done!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)