import schedule
import time
from datetime import datetime
import subprocess
import logging
import os

LOG_DIR = os.path.join(os.getcwd(), "logs") # 로그 폴더 경로
LOG_FILE = os.path.join(LOG_DIR, "crawling_scheduler.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s %(message)s"
)

def run_crawler():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        crawler = os.path.join(current_dir, "crawling.py")
        
        result = subprocess.run(["python", crawler], capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(result.stdout)
        else:
            logging.error(f">>>> 에러 발생: {str(result.stdout)}")

    except Exception as e:
        logging.error(f">>>> 에러 발생: {str(e)}")

def check_and_run_crawler():
    current_date = datetime.now()
    
    if current_date.month in [2, 5, 8, 11] and current_date.day == 1 and current_date.hour == 0: # 2, 5, 8, 11월의 1일 0시
        logging.info(f">>>> 스케쥴러 실행")
        run_crawler()

def setup_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR) # 로그 폴더 생성

def main():
    setup_log_dir()
    while True:
        check_and_run_crawler()
        time.sleep(3600) 

if __name__ == "__main__":
    main()