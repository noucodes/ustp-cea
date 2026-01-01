import requests
from bs4 import BeautifulSoup
import os
from dotenv import load_dotenv
import logging
from datetime import datetime
import json
import time
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ---------------------
# Load environment variables
# ---------------------
load_dotenv(override=True)

# ---------------------
# Debug Mode & Config
# ---------------------
DEBUG_MODE = os.getenv("DEBUG_MODE", "False").lower() == "true"
DELAY_BETWEEN_REQUESTS = float(os.getenv("DELAY_BETWEEN_REQUESTS", "0.5"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))  # Number of parallel workers

# ---------------------
# MULTI-DEPARTMENT CONFIG
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
# Logging Configuration
# ---------------------
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"scraper_multi_dept_{timestamp}.log"
log_level = logging.DEBUG if DEBUG_MODE else logging.INFO

logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
if DEBUG_MODE:
    logger.debug("="*60)
    logger.debug("DEBUG MODE ENABLED")
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

# Main session for enrollment fetching
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# Thread-safe lock for file writing
write_lock = threading.Lock()

# ---------------------
# Helper Functions
# ---------------------
def safe_bs_parse(html_content: str) -> BeautifulSoup:
    """Safely parse HTML content"""
    return BeautifulSoup(html_content, "html.parser")

def extract_student_info(student: Dict[str, Any]) -> Dict[str, Any]:
    """Extract student information from enrollment data"""
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
    """Categorize year level from text"""
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

def save_jsonl(data: List[Dict], filename: str, mode: str = 'w'):
    """Save data to JSONL file"""
    dir_path = os.path.dirname(filename)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    try:
        with write_lock:
            with open(filename, mode, encoding='utf-8') as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
        if mode == 'w':
            logger.info(f"💾 Saved {len(data)} records → {filename}")
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")

def append_single_record(record: Dict, filename: str):
    """Append a single record to JSONL file (thread-safe)"""
    dir_path = os.path.dirname(filename)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)
    
    try:
        with write_lock:
            with open(filename, 'a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
    except Exception as e:
        logger.error(f"Failed to append to {filename}: {e}")

def get_fresh_csrf(sess: requests.Session = None) -> Optional[str]:
    """Get fresh CSRF token"""
    if sess is None:
        sess = session
    try:
        resp = sess.get(LOGIN_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        token = soup.find("meta", {"name": "csrf-token"})
        return token["content"] if token else None
    except Exception as e:
        logger.error(f"Failed to get CSRF token: {e}")
        return None

# ---------------------
# Login
# ---------------------
def login() -> bool:
    """Login to the system"""
    logger.info("🔐 Logging in...")
    csrf_token = get_fresh_csrf()
    
    if not csrf_token:
        logger.error("❌ Failed to get CSRF token")
        return False
    
    login_payload = {
        "_token": csrf_token,
        "Username": USERNAME,
        "password": PASSWORD,
    }
    
    try:
        resp = session.post(LOGIN_URL, data=login_payload, timeout=15)
        resp.raise_for_status()
        
        if "dashboard" in resp.url.lower() or resp.status_code == 200:
            logger.info("✅ Login successful!")
            return True
        else:
            logger.error("❌ Login failed - unexpected response")
            return False
    except Exception as e:
        logger.error(f"❌ Login failed: {e}")
        return False

# ---------------------
# Fetch Enrollment
# ---------------------
def fetch_enrollment(prog_id: str) -> List[Dict]:
    """Fetch enrollment data for a program"""
    csrf_token = get_fresh_csrf()
    if not csrf_token:
        logger.error("Could not get CSRF token for enrollment")
        return []
    
    enrollment_params = {
        "event": "registered",
        "level": "-1",
        "term": "187",
        "campus": "1",
        "progid": prog_id,
        "validation_status": "0",
        "section": "",
        "draw": "1",
        "start": "0",
        "length": "-1",
        "_": str(int(time.time() * 1000))
    }
    
    enrollment_headers = {
        "X-CSRF-TOKEN": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
    }
    
    try:
        resp = session.get(ENROLLMENT_URL, params=enrollment_params, headers=enrollment_headers, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        students = data.get("data", [])
        logger.info(f"📋 Fetched {len(students)} students for progid {prog_id}")
        return students
    except Exception as e:
        logger.error(f"❌ Failed to fetch enrollment: {e}")
        return []

# ---------------------
# Fetch Student Grades (Worker Thread Function)
# ---------------------
def fetch_student_grades_worker(encoded_id: str, student_id: str) -> List[Dict]:
    """Fetch grades for a single student (worker thread - creates own session)"""
    # Create a new session for this worker thread
    worker_session = requests.Session()
    worker_session.headers.update(session.headers)
    
    # Get fresh CSRF and login
    csrf_token = get_fresh_csrf(worker_session)
    if not csrf_token:
        return []
    
    # Login with worker session
    try:
        login_payload = {"_token": csrf_token, "Username": USERNAME, "password": PASSWORD}
        worker_session.post(LOGIN_URL, data=login_payload, timeout=15)
    except:
        worker_session.close()
        return []
    
    # Get fresh CSRF for transcript request
    csrf_token = get_fresh_csrf(worker_session)
    if not csrf_token:
        worker_session.close()
        return []
    
    payload = {
        "_token": csrf_token,
        "event": "load-grades",
        "progClass": "50",
        "idno": encoded_id
    }
    
    headers = {
        "X-CSRF-TOKEN": csrf_token,
        "X-Requested-With": "XMLHttpRequest",
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            resp = worker_session.post(TRANSCRIPT_URL, data=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            grade_data = resp.json()
            
            if grade_data.get("error"):
                logger.warning(f"Server error for {student_id}: {grade_data.get('message', 'Unknown')}")
                worker_session.close()
                return []
            
            # Parse grades table
            html_content = grade_data.get("content", "")
            if not html_content.strip():
                worker_session.close()
                return []
            
            soup = BeautifulSoup(html_content, "html.parser")
            table = soup.find("table", {"id": "tblhistory"}) or soup.find("table")
            
            if not table:
                worker_session.close()
                return []
            
            grades = []
            
            for row in table.find_all("tr")[1:]:  # Skip header
                cols = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                
                # Skip invalid rows
                if len(cols) < 8:
                    continue
                
                # Skip header/separator rows
                if cols[0] in ["", "Year", "Semester", "Term"] or cols[1] in ["Midterm", "Final", "Re-Exam", "ACADEMICYEAR", "COURSE"]:
                    continue
                
                # Valid subject row - extract data
                if cols[0] and cols[1]:
                    # Check if row starts with "1." - this means there's an extra academic year column
                    if cols[0] == "1.":
                        # Structure: ['1.', 'academic_year', 'subject_code', 'description', 'units', ..., grade at index 9]
                        grades.append({
                            "subject_code": cols[2],
                            "subject_description": cols[3] if len(cols) > 3 else "",
                            "subject_unit": cols[4] if len(cols) > 4 else "",
                            "grade": cols[9] if len(cols) > 9 else ""
                        })
                    else:
                        # Normal structure: ['2.', 'subject_code', 'description', 'units', ..., grade at index 8]
                        grades.append({
                            "subject_code": cols[1],
                            "subject_description": cols[2] if len(cols) > 2 else "",
                            "subject_unit": cols[3] if len(cols) > 3 else "",
                            "grade": cols[8] if len(cols) > 8 else ""
                        })
            
            worker_session.close()
            return grades
            
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{MAX_RETRIES} failed for {student_id}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2)
            continue
    
    logger.error(f"❌ All retries failed for {student_id}")
    worker_session.close()
    return []

# ---------------------
# Post-processing: Duplicates & Status Classification
# ---------------------
def classify_enrollment_status(grades: List[Dict]) -> Tuple[str, List[str]]:
    """Classify student enrollment status based on grades"""
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
    """Remove duplicate subjects and classify enrollment status"""
    grades = student.get("grades", [])
    student_id = student.get("student_id", "Unknown")
    name = student.get("name", "No name")

    # Find duplicates by subject code
    seen = defaultdict(list)
    for i, g in enumerate(grades):
        code = g.get("subject_code", "").strip().upper()
        if code:
            seen[code].append(i)

    # Remove earlier duplicates (keep last occurrence)
    to_remove = set()
    for code, indices in seen.items():
        if len(indices) > 1:
            removed = len(indices) - 1
            to_remove.update(indices[:-1])
            logger.info(f"Duplicate '{code}' in {student_id} ({name}) → removed {removed} entries")

    if to_remove:
        student["grades"] = [g for i, g in enumerate(grades) if i not in to_remove]

    # Classify enrollment status
    status, reasons = classify_enrollment_status(student["grades"])
    student["enrollment_status"] = status

    # Log status
    if status == "Grades Pending":
        logger.warning(f"GRADES PENDING: {student_id} - {name} | {len(reasons)} blanks")
    elif status == "Irregular":
        irregular_count = len([r for r in reasons if 'blank' not in r])
        logger.warning(f"IRREGULAR: {student_id} - {name} | {irregular_count} failing/incomplete grades")
    else:
        logger.info(f"REGULAR: {student_id} - {name}")

    return student

# ---------------------
# Main Processing (WITH MULTI-THREADING)
# ---------------------
def process_department(dept_name: str, folder_name: str, prog_id: str, base_dir: str) -> Counter:
    """Process a single department with multi-threading"""
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
    live_output_file = os.path.join(dept_folder, f"live_output_{ts}.jsonl")
    
    # Fetch enrollment
    students_raw = fetch_enrollment(prog_id)
    if not students_raw:
        logger.warning(f"No students found for {dept_name}")
        return Counter()
    
    # Extract student info
    student_infos = []
    for s in students_raw:
        info = extract_student_info(s)
        if info.get("encoded_id"):
            student_infos.append(info)
    
    total = len(student_infos)
    if total == 0:
        logger.warning(f"No valid students for {dept_name}")
        return Counter()
    
    logger.info(f"🚀 Processing {total} students in {dept_name} with {MAX_WORKERS} parallel workers")
    
    # Process with multi-threading
    raw_records = []
    processed = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_info = {
            executor.submit(fetch_student_grades_worker, info["encoded_id"], info["student_id"]): info 
            for info in student_infos
        }
        
        # Process completed tasks
        for future in as_completed(future_to_info):
            info = future_to_info[future]
            processed += 1
            
            try:
                grades = future.result()
            except Exception as e:
                logger.error(f"Error processing {info['student_id']}: {e}")
                grades = []
            
            record = {
                "student_id": info["student_id"],
                "name": info["name"],
                "course": info["course"],
                "year_level": info["year_level"],
                "total_subjects": len(grades),
                "grades": grades
            }
            raw_records.append(record)
            
            # LIVE OUTPUT: Append to file immediately
            append_single_record(record, live_output_file)
            
            # Console output with live progress
            status_symbol = "✓" if len(grades) > 0 else "✗"
            print(f"[{dept_name}] [{processed}/{total}] {status_symbol} {info['student_id']:12} | {info['name']:30} | {len(grades):3} subjects")
            
            time.sleep(DELAY_BETWEEN_REQUESTS)
    
    # Save raw data
    save_jsonl(raw_records, raw_file, 'w')
    
    # Post-process: remove duplicates and classify
    logger.info(f"📊 Post-processing {dept_name}...")
    final_records = [fix_duplicates_and_classify(r) for r in raw_records]
    save_jsonl(final_records, final_file, 'w')
    
    # Split by status
    regular = [r for r in final_records if r["enrollment_status"] == "Regular"]
    irregular = [r for r in final_records if r["enrollment_status"] == "Irregular"]
    pending = [r for r in final_records if r["enrollment_status"] == "Grades Pending"]
    
    save_jsonl(regular, regular_file, 'w')
    save_jsonl(irregular, irregular_file, 'w')
    save_jsonl(pending, pending_file, 'w')
    
    # Summary
    dept_counter = Counter(r["enrollment_status"] for r in final_records)
    students_with_no_grades = sum(1 for r in raw_records if r["total_subjects"] == 0)
    
    print(f"\n{'='*100}")
    print(f"✅ {dept_name} COMPLETED")
    print(f"{'='*100}")
    print(f"  Total students:          {len(final_records)}")
    print(f"  Regular:                 {dept_counter['Regular']}")
    print(f"  Irregular:               {dept_counter['Irregular']}")
    print(f"  Grades Pending:          {dept_counter['Grades Pending']}")
    print(f"  No grades retrieved:     {students_with_no_grades}")
    print(f"  Live output file:        {live_output_file}")
    print(f"  Files saved to:          ./scraped_data/{folder_name}/")
    print(f"{'='*100}\n")
    
    return dept_counter

# ---------------------
# Main Execution
# ---------------------
def main():
    logger.info("🚀 USTP Multi-Department Grades Scraper (Multi-Threaded) - Starting...")
    logger.info(f"⚡ Using {MAX_WORKERS} parallel workers per department")
    
    # Login
    if not login():
        logger.error("Login failed. Exiting.")
        return
    
    base_dir = "scraped_data"
    os.makedirs(base_dir, exist_ok=True)
    
    global_summary = Counter()
    start_time = time.time()
    
    # Process each department
    for dept_name, folder_name, prog_id in DEPARTMENTS:
        dept_counter = process_department(dept_name, folder_name, prog_id, base_dir)
        global_summary.update(dept_counter)
    
    elapsed_time = time.time() - start_time
    
    # Global summary
    print("\n" + "="*100)
    print("🎉 ALL DEPARTMENTS PROCESSED")
    print("="*100)
    print(f"TOTAL ACROSS ALL DEPARTMENTS:")
    print(f"  Regular:         {global_summary['Regular']}")
    print(f"  Irregular:       {global_summary['Irregular']}")
    print(f"  Grades Pending:  {global_summary['Grades Pending']}")
    print(f"  Grand Total:     {sum(global_summary.values())}")
    print(f"\n⏱️  Total time:       {elapsed_time/60:.2f} minutes")
    print(f"📁 Output folder:     ./scraped_data/")
    print(f"📝 Log file:          {log_filename}")
    print("="*100)
    
    logger.info("✅ Multi-department scraping completed successfully!")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n👋 Script interrupted by user")
    except Exception as e:
        logger.error(f"💥 Fatal error: {e}", exc_info=DEBUG_MODE)