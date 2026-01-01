import json
import logging
from datetime import datetime
from collections import defaultdict
from typing import List, Dict, Optional

# ---------------------
# Logging Setup
# ---------------------
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_filename = f"gwa_calculation_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------------------
# GWA Calculation Logic
# ---------------------
def is_valid_grade(grade: str) -> bool:
    """Check if grade is a valid numerical grade (1.0 to 5.0)"""
    try:
        g = float(grade)
        return 1.0 <= g <= 5.0
    except (ValueError, TypeError):
        return False

def calculate_gwa(student: Dict) -> Dict:
    """
    Calculate GWA using only valid numerical grades.
    Excludes: "", INC, W, D/F, 5 (if you want to exclude failures too — optional)
    """
    grades = student.get("grades", [])
    student_id = student.get("student_id", "Unknown")
    name = student.get("name", "No name")

    total_units = 0.0
    total_grade_points = 0.0
    valid_subjects = 0

    for subject in grades:
        grade_str = str(subject.get("grade", "")).strip()
        units_str = subject.get("subject_unit", "0")

        # Convert units (could be "3", "3.0", or "(3)")
        try:
            units = float(str(units_str).replace("(", "").replace(")", ""))
        except:
            units = 0.0

        if not is_valid_grade(grade_str):
            continue  # skip non-numerical or incomplete grades

        grade = float(grade_str)
        # Optional: Exclude failed grades (5.0) from GWA? Uncomment next line if yes
        # if grade >= 5.0: continue

        total_units += units
        total_grade_points += grade * units
        valid_subjects += 1

    if total_units > 0:
        gwa = round(total_grade_points / total_units, 3)
    else:
        gwa = None

    student["gwa"] = gwa
    student["total_units_completed"] = round(total_units, 1)
    student["total_valid_subjects"] = valid_subjects
    student["total_grade_points"] = round(total_grade_points, 3)

    status = "No valid grades"
    if gwa:
        if gwa <= 1.50:
            status = "With Honors (Possible Summa/Magna)"
        elif gwa <= 1.75:
            status = "With High Honors"
        elif gwa <= 2.00:
            status = "With Honors"

    logger.info(
        f"{student_id} | {name:<25} | GWA: {gwa if gwa else 'N/A':<6} | "
        f"Units: {round(total_units,1)} | Subjects: {valid_subjects}"
    )

    if gwa and gwa <= 2.00:
        logger.info(f"🎖️ HONOR STUDENT: {name} - GWA {gwa}")

    return student

# ---------------------
# Main Processing
# ---------------------
def process_gwa(input_file: str, output_file: str):
    if not input_file.endswith(".jsonl"):
        logger.error("Input file must be a .jsonl file")
        return

    students = []
    with open(input_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                student = json.loads(line)
                students.append(student)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON on line {line_num}: {e}")

    logger.info(f"Loaded {len(students)} students from {input_file}")

    # Calculate GWA for all
    enriched_students = [calculate_gwa(s) for s in students]

    # Save new file
    with open(output_file, "w", encoding="utf-8") as f:
        for s in enriched_students:
            f.write(json.dumps(s, ensure_ascii=False) + "\n")

    # Summary by year level and status
    stats = defaultdict(lambda: {"count": 0, "gwa_sum": 0, "honors": 0})
    overall_gwa_list = []

    for s in enriched_students:
        year = s.get("year_level", "Unknown")
        gwa = s.get("gwa")
        status = s.get("enrollment_status", "Unknown")

        if gwa:
            stats[year]["count"] += 1
            stats[year]["gwa_sum"] += gwa
            overall_gwa_list.append(gwa)
            if gwa <= 2.00:
                stats[year]["honors"] += 1

    logger.info("\n" + "="*60)
    logger.info("GWA CALCULATION SUMMARY")
    logger.info("="*60)
    for year in sorted(stats.keys()):
        data = stats[year]
        if data["count"] > 0:
            avg_gwa = data["gwa_sum"] / data["count"]
            logger.info(f"{year:<12} | Avg GWA: {avg_gwa:.3f} | Students: {data['count']} | Honors: {data['honors']}")

    if overall_gwa_list:
        overall_avg = sum(overall_gwa_list) / len(overall_gwa_list)
        logger.info(f"\nOverall Average GWA: {overall_avg:.3f}")
        logger.info(f"Total with computed GWA: {len(overall_gwa_list)}")

    print("\n" + "="*70)
    print("GWA CALCULATION COMPLETED")
    print("="*70)
    print(f"Input:      {input_file}")
    print(f"Output:     {output_file}")
    print(f"Students:   {len(students)}")
    print(f"Log file:   {log_filename}")
    print("="*70)

# ---------------------
# Run
# ---------------------
if __name__ == "__main__":
    import glob
    import os

    # Auto-detect latest grades_final_with_status file
    pattern = "grades_final_with_status_*.jsonl"
    files = sorted(glob.glob(pattern), reverse=True)

    if not files:
        print(f"No file matching '{pattern}' found in current directory.")
        print("Place this script in the same folder as your scraper output.")
        exit(1)

    latest_file = files[0]
    print(f"Found latest file: {latest_file}")

    output_filename = f"grades_with_gwa_{timestamp}.jsonl"

    process_gwa(latest_file, output_filename)