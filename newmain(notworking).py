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
from collections import defaultdict, Counter

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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "3"))  # Reduced from 5 to avoid overwhelming server

USERNAME = os.getenv("USERNAME1")
PASSWORD = os.getenv("PASSWORD")

if not USERNAME or not PASSWORD:
    print("ERROR: Missing USERNAME1 or PASSWORD in .env file!")
    exit(1)

# ---------------------
# MULTI-DEPARTMENT CONFIG: [Display Name, Folder Name, PROG_ID]
# ---------------------
DEPARTMENTS = [
    ("Civil Engineering",      "Civil_Engineering",      "eUd1UTFQVzZpbGtUTjFQUXl2T1dvdz09"),
    ("Architecture",           "Architecture",           "dkcwSThiVUcwcjl5Wit5blMzRVNQUT09"),
    ("Mechanical Engineering", "Mechanical_Engineering", "MHBwWW05Zm9maVJDcXV5VWxqQ0JhZz09"),
    ("Electrical Engineering", "Electrical_Engineering", "YXdMZWhIVTZLeGpRUTByM2RmRExEQT09"),
    ("Computer Engineering",   "Computer_Engineering",   "cTFEaXV4RTUrQjkrT1VhMnp6aWNXQT09"),
    ("Electronics Engineering","Electronics_Engineering","Qm9YMkNhSEIzZVY0blBFVUJNZTlDUT09"),
    ("Geodetic Engineering",   "Geodetic_Engineering",   "aUYyZGF0S01QeHg1OTVVbEZUYXlwdz09"),
]

# ---------------------
# Logging Setup
# ---------------------
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"scraper_multi_dept_{timestamp}.log"

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
    """Save to JSONL (overwrite by default, 'a' to append)"""
    # Only create directory if filename has a directory component
    dir_path = os.path.dirname(filename)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    try:
        with open(filename, mode, encoding='utf-8') as f:
            for item in data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
        if mode == 'w':
            logger.info(f"Saved {len(data)} records → {filename}")
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")

def get_fresh_csrf(sess: requests.Session, timeout: int = 15) -> Optional[str]:
    """Get a fresh CSRF token with configurable timeout"""
    try:
        resp = sess.get(LOGIN_URL, timeout=timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        token = soup.find("meta", {"name": "csrf-token"})
        return token["content"] if token else None
    except requests.exceptions.Timeout as e:
        logger.error(f"CSRF fetch timeout after {timeout}s: {e}")
        return None
    except Exception as e:
        logger.error(f"CSRF fetch failed: {e}")
        return None

# ---------------------
# Login (shared)
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
# Fetch Enrollment for a program
# ---------------------
def fetch_enrollment(prog_id: str) -> List[Dict]:
    # Get fresh CSRF with increased timeout
    csrf = get_fresh_csrf(session, timeout=20)
    if not csrf:
        logger.error(f"Could not get CSRF token for enrollment fetch")
        return []
    
    params = {
        "event": "registered", "level": "-1", "term": "187", "campus": "1",
        "progid": prog_id, "validation_status": "0", "section": "", "draw": "1",
        "start": "0", "length": "-1", "_": str(int(time.time() * 1000))
    }
    headers = {"X-CSRF-TOKEN": csrf, "X-Requested-With": "XMLHttpRequest"}

    try:
        resp = session.get(ENROLLMENT_URL, params=params, headers=headers, timeout=30)  # Increased timeout
        resp.raise_for_status()
        data = resp.json()
        students = data.get("data", [])
        logger.info(f"Fetched {len(students)} enrolled students for progid {prog_id}")
        return students
    except Exception as e:
        logger.error(f"Enrollment fetch failed for progid {prog_id}: {e}")
        return []

# ---------------------
# Fetch Grades (worker) - WITH IMPROVED RETRY MECHANISM
# ---------------------
def fetch_student_grades(encoded_id: str, retry_count: int = 0) -> List[Dict]:
    """
    Fetch grades for a single student with retry mechanism.
    Creates its own session and logs in independently to avoid concurrency issues.
    
    Args:
        encoded_id: The student's encoded ID
        retry_count: Current retry attempt (0 = first attempt)
    
    Returns:
        List of grade dictionaries
    """
    max_retries = MAX_RETRIES
    retry_delay = 3  # Start with 3 seconds delay
    
    # Add extra delay between retries to avoid overwhelming server
    if retry_count > 0:
        time.sleep(retry_delay * retry_count)
    
    worker_session = requests.Session()
    worker_session.headers.update(session.headers)
    
    try:
        # Get fresh CSRF token for this worker with increased timeout
        csrf = get_fresh_csrf(worker_session, timeout=20)
        if not csrf:
            logger.debug(f"[DEBUG] No CSRF token for student {encoded_id}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - no CSRF")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []

        # Login with this worker session
        try:
            login_resp = worker_session.post(
                LOGIN_URL, 
                data={"_token": csrf, "Username": USERNAME, "password": PASSWORD}, 
                timeout=20  # Increased timeout
            )
            login_resp.raise_for_status()
        except requests.exceptions.Timeout as e:
            logger.debug(f"[DEBUG] Worker login timeout for {encoded_id}: {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - login timeout")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []
        except Exception as e:
            logger.debug(f"[DEBUG] Worker login failed for {encoded_id}: {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - login error")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []

        # Get a fresh CSRF token for the transcript request with increased timeout
        csrf_transcript = get_fresh_csrf(worker_session, timeout=20)
        if not csrf_transcript:
            logger.debug(f"[DEBUG] No transcript CSRF token for student {encoded_id}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - no transcript CSRF")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []

        payload = {
            "_token": csrf_transcript, 
            "event": "load-grades", 
            "progClass": "50", 
            "idno": encoded_id
        }
        headers = {
            "X-CSRF-TOKEN": csrf_transcript, 
            "X-Requested-With": "XMLHttpRequest"
        }

        try:
            resp = worker_session.post(TRANSCRIPT_URL, data=payload, headers=headers, timeout=25)  # Increased timeout
            resp.raise_for_status()
            
            # Parse JSON response
            json_data = resp.json()
            content = json_data.get("content", "")
            
            if not content.strip():
                logger.debug(f"[DEBUG] Empty content returned for student {encoded_id}")
                return []

            # Parse HTML content
            soup = BeautifulSoup(content, "html.parser")
            table = soup.find("table", {"id": "tblhistory"}) or soup.find("table")

            if not table:
                logger.debug(f"[DEBUG] No table found in transcript for student {encoded_id}")
                return []

            grades = []
            row_count = 0

            if DEBUG_MODE:
                logger.debug(f"[DEBUG] === START TRANSCRIPT PARSING FOR {encoded_id} ===")
            
            # Parse table rows (skip header row)
            for row in table.find_all("tr")[1:]:
                row_count += 1
                cols = [c.get_text(strip=True) for c in row.find_all(["td", "th"])]
                
                # Debug output for first few rows or when DEBUG_MODE is on
                if DEBUG_MODE and row_count <= 10:
                    logger.debug(f"[DEBUG] Row {row_count:02d} | Cols: {len(cols)} | Content: {cols[:8] if len(cols) >= 8 else cols}")

                # Valid grade row must have:
                # 1. At least 8 columns
                # 2. Non-empty subject code in column 1 (index 1)
                # 3. Column 0 should not be just "1." or empty (these are usually separators)
                if len(cols) >= 8 and cols[1].strip():
                    # Additional check: column 0 should contain year/semester info or be a number > 1
                    col0 = cols[0].strip()
                    
                    # Skip rows that are clearly not grade entries
                    if col0 in ["", "Year", "Semester", "Term"]:
                        if DEBUG_MODE:
                            logger.debug(f"[DEBUG] SKIPPED row {row_count}: separator or header (col0='{col0}')")
                        continue
                    
                    grade_entry = {
                        "subject_code": cols[1],
                        "subject_description": cols[2],
                        "subject_unit": cols[3] if len(cols) > 3 else "",
                        "grade": cols[7] if len(cols) > 7 else ""
                    }

                    if col0=="1.":
                        grade_entry = {
                        "subject_code": cols[2],
                        "subject_description": cols[3],
                        "subject_unit": cols[4] if len(cols) > 4 else "",
                        "grade": cols[8] if len(cols) > 8 else ""
                    }
                    grades.append(grade_entry)
                    
                    if DEBUG_MODE:
                        logger.debug(f"[DEBUG] ADDED SUBJECT: {cols[1]} | Grade: {cols[7] if len(cols)>7 else 'N/A'}")
                else:
                    if DEBUG_MODE:
                        logger.debug(f"[DEBUG] SKIPPED row {row_count}: cols={len(cols)}, subject_code='{cols[1] if len(cols)>1 else ''}'")

            if DEBUG_MODE:
                logger.debug(f"[DEBUG] === END PARSING FOR {encoded_id} | Found {len(grades)} valid subjects out of {row_count} rows ===")

            return grades

        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout fetching grades for {encoded_id}: {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - request timeout")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error fetching grades for {encoded_id}: {e}")
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - request error")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []
        except json.JSONDecodeError as e:
            logger.warning(f"JSON decode error for {encoded_id}: {e}")
            # Don't retry JSON errors - likely a server-side issue
            return []
        except Exception as e:
            logger.warning(f"Unexpected error fetching grades for {encoded_id}: {e}")
            if DEBUG_MODE:
                logger.debug(f"[DEBUG] Exception details:", exc_info=True)
            if retry_count < max_retries:
                logger.info(f"Retrying student {encoded_id} (attempt {retry_count + 1}/{max_retries}) - unexpected error")
                worker_session.close()
                return fetch_student_grades(encoded_id, retry_count + 1)
            return []
    finally:
        worker_session.close()
    
# ---------------------
# Post-processing: 3-way status + duplicates
# ---------------------
def classify_enrollment_status(grades: List[Dict]) -> Tuple[str, List[str]]:
    """
    Classify student enrollment status based on their grades.
    Returns: (status, list_of_reasons)
    """
    pending_reasons = []
    irregular_reasons = []

    for i, g in enumerate(grades):
        grade = str(g.get("grade", "")).strip()
        if grade == "":
            pending_reasons.append(f"index {i}: blank grade")
        elif grade in {"5", "5.0", "5.00", "INC", "W", "D/F"}:
            irregular_reasons.append(f"index {i}: '{grade}'")

    if pending_reasons:
        return "Grades Pending", pending_reasons
    elif irregular_reasons:
        return "Irregular", irregular_reasons
    else:
        return "Regular", []

def fix_duplicates_and_classify(student: Dict) -> Dict:
    """
    Remove duplicate subject entries (keep the last occurrence)
    and classify enrollment status.
    """
    grades = student.get("grades", [])
    student_id = student.get("student_id", "Unknown")
    name = student.get("name", "No name")

    # Find duplicates by subject code
    seen = defaultdict(list)
    for i, g in enumerate(grades):
        code = g.get("subject_code", "").strip().upper()
        if code:
            seen[code].append(i)

    # Mark earlier duplicates for removal (keep last occurrence)
    to_remove = set()
    for code, indices in seen.items():
        if len(indices) > 1:
            removed = len(indices) - 1
            to_remove.update(indices[:-1])  # Remove all except the last
            logger.info(f"Duplicate '{code}' in {student_id} ({name}) → removed {removed} entries")

    # Remove duplicates
    if to_remove:
        student["grades"] = [g for i, g in enumerate(grades) if i not in to_remove]

    # Classify enrollment status
    status, reasons = classify_enrollment_status(student["grades"])
    student["enrollment_status"] = status

    # Log status
    if status == "Grades Pending":
        logger.warning(f"GRADES PENDING: {student_id} - {name} | {len(reasons)} blanks")
    elif status == "Irregular":
        logger.warning(f"IRREGULAR: {student_id} - {name} | {len(irregular_reasons := [r for r in reasons if 'blank' not in r])} failing/incomplete grades")
    else:
        logger.info(f"REGULAR: {student_id} - {name}")

    return student

# ---------------------
# Main: Multi-Department Loop
# ---------------------
def main():
    logger.info("USTP Multi-Department Grades Scraper v4.1 - Starting...")

    if not login():
        logger.error("Login failed. Exiting.")
        return

    base_dir = "scraped_data"
    os.makedirs(base_dir, exist_ok=True)

    global_summary = Counter()

    for dept_name, folder_name, prog_id in DEPARTMENTS:
        logger.info(f"\n{'='*100}")
        logger.info(f"PROCESSING: {dept_name} ({prog_id})")
        logger.info(f"{'='*100}")

        dept_folder = os.path.join(base_dir, folder_name)
        os.makedirs(dept_folder, exist_ok=True)

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')

        raw_file = os.path.join(dept_folder, f"grades_complete_{ts}.jsonl")
        final_file = os.path.join(dept_folder, f"grades_final_with_status_{ts}.jsonl")
        regular_file = os.path.join(dept_folder, f"grades_regular_{ts}.jsonl")
        irregular_file = os.path.join(dept_folder, f"grades_irregular_{ts}.jsonl")
        pending_file = os.path.join(dept_folder, f"grades_pending_{ts}.jsonl")

        # Fetch enrollment
        students_raw = fetch_enrollment(prog_id)
        if not students_raw:
            logger.warning(f"No students found for {dept_name}. Skipping.")
            continue

        # Extract student info
        student_infos = []
        for s in students_raw:
            info = extract_student_info(s)
            if info.get("encoded_id"):
                student_infos.append(info)

        total = len(student_infos)
        if total == 0:
            logger.warning(f"No valid students with encoded_id for {dept_name}")
            continue

        logger.info(f"Scraping grades for {total} students in {dept_name}")

        raw_records = []
        failed_students = []  # Track students that failed even after retries
        processed = 0

        # Process students in parallel
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(fetch_student_grades, info["encoded_id"]): info for info in student_infos}

            for future in as_completed(futures):
                info = futures[future]
                processed += 1
                
                try:
                    grades = future.result()
                except Exception as e:
                    logger.error(f"Error processing {info['student_id']}: {e}")
                    grades = []
                    failed_students.append({
                        "student_id": info["student_id"],
                        "name": info["name"],
                        "encoded_id": info["encoded_id"],
                        "error": str(e)
                    })

                record = {
                    "student_id": info["student_id"],
                    "name": info["name"],
                    "course": info["course"],
                    "year_level": info["year_level"],
                    "total_subjects": len(grades),
                    "grades": grades
                }
                raw_records.append(record)

                status_indicator = "✓" if len(grades) > 0 else "✗"
                logger.info(f"[{dept_name}] [{processed}/{total}] {status_indicator} {info['student_id']} → {len(grades)} subjects")
                time.sleep(DELAY_BETWEEN_REQUESTS)

        # Save raw data (overwrite, not append)
        save_jsonl(raw_records, raw_file, 'w')

        # Post-process: remove duplicates and classify
        logger.info(f"Post-processing {dept_name}...")
        final_records = [fix_duplicates_and_classify(r) for r in raw_records]
        save_jsonl(final_records, final_file, 'w')

        # Split by status
        regular = [r for r in final_records if r["enrollment_status"] == "Regular"]
        irregular = [r for r in final_records if r["enrollment_status"] == "Irregular"]
        pending = [r for r in final_records if r["enrollment_status"] == "Grades Pending"]

        save_jsonl(regular, regular_file, 'w')
        save_jsonl(irregular, irregular_file, 'w')
        save_jsonl(pending, pending_file, 'w')

        # Save failed students list if any
        if failed_students:
            failed_file = os.path.join(dept_folder, f"failed_students_{ts}.jsonl")
            save_jsonl(failed_students, failed_file, 'w')
            logger.warning(f"⚠ {len(failed_students)} students failed to fetch even after {MAX_RETRIES} retries")

        # Department summary
        dept_counter = Counter(r["enrollment_status"] for r in final_records)
        global_summary.update(dept_counter)

        students_with_no_grades = sum(1 for r in raw_records if r["total_subjects"] == 0)

        print(f"\n>>> {dept_name} COMPLETED")
        print(f"    Total: {len(final_records)} | Regular: {dept_counter['Regular']} | Irregular: {dept_counter['Irregular']} | Pending: {dept_counter['Grades Pending']}")
        print(f"    Students with no grades: {students_with_no_grades}")
        if failed_students:
            print(f"    ⚠ Failed to fetch: {len(failed_students)} students (see failed_students_{ts}.jsonl)")
        print(f"    Files → ./scraped_data/{folder_name}/")

    # Global summary
    print("\n" + "="*100)
    print("ALL DEPARTMENTS PROCESSED")
    print("="*100)
    print(f"TOTAL ACROSS ALL DEPARTMENTS:")
    print(f"   Regular:         {global_summary['Regular']}")
    print(f"   Irregular:       {global_summary['Irregular']}")
    print(f"   Grades Pending:  {global_summary['Grades Pending']}")
    print(f"   Grand Total:     {sum(global_summary.values())}")
    print(f"Output folder:      ./scraped_data/")
    print(f"Log file:           {log_filename}")
    print("="*100)

    logger.info("Multi-department scraping completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\nScript interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)