from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import shutil
import time
import glob
import logging

DOWNLOAD_DIRECTORY = os.path.join(os.getcwd(), "downloads") # 다운로드 경로

def setup_download_directory():
    if os.path.exists(DOWNLOAD_DIRECTORY):
        shutil.rmtree(DOWNLOAD_DIRECTORY) # 기존 다운로드 폴더 삭제
        os.makedirs(DOWNLOAD_DIRECTORY) # 다운로드 폴더 생성

def setup_chrome_options(chrome_options):
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIRECTORY,
        "download.prompt_for_download": False
    })

    return chrome_options

def setup_webdriver():
    setup_download_directory() # 다운로드 폴더 설정

    chrome_options = webdriver.ChromeOptions()
    setup_chrome_options(chrome_options) # 크롬 옵션 설정

    driver = webdriver.Chrome(options=chrome_options) # 웹드라이버 생성

    return driver

def wait_for_download():
    file = os.path.join(DOWNLOAD_DIRECTORY, "소상공인시장진흥공단_상가(상권)정보_*.zip")
    while not glob.glob(file):
        time.sleep(1)

def main():
    try:
        driver = setup_webdriver()
        driver.get("https://www.data.go.kr/index.do")

        search_input = driver.find_element(By.CSS_SELECTOR, "input#keyword") # 검색 입력
        search_input.send_keys("소상공인시장진흥공단_상가(상권)정보")

        search_button = driver.find_element(By.CSS_SELECTOR, "button.btn-search") # 검색 버튼
        search_button.click()
    
        first_element_download_button = driver.find_element(By.CSS_SELECTOR, "#fileDataList > div.result-list > ul > li:nth-child(1) > div.bottom-area > a") # 다운로드 버튼
        first_element_download_button.click()

        wait_for_download()
        file = os.listdir(DOWNLOAD_DIRECTORY)[0]

        logging.info(f">>>> 파일 다운로드: {file}")

    except Exception as e:
        logging.error(f">>>> 파일 다운로드 실패")
        logging.error(str(e))
        
    finally:
        driver.quit()

if __name__ == "__main__":
    main()
