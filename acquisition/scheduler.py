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

def setup_log_dir():
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

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

if __name__ == "__main__":
    setup_log_dir()
    run_crawler()
