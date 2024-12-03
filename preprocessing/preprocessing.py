from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
import logging
import subprocess
import os

# 경로 설정
HDFS_INPUT_DIR = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/team8/data"
HDFS_OUTPUT_DIR = "hdfs://sandbox-hdp.hortonworks.com:8020/user/maria_dev/team8/preprocessing/preprocessed_data"
LOG_DIR = os.path.join(os.getcwd(), "preprocessing_logs")
LOG_FILE = os.path.join(LOG_DIR, "preprocessing.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)


def ensure_hdfs_directory(hdfs_path):
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-test", "-d", hdfs_path],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            subprocess.run(
                ["hdfs", "dfs", "-mkdir", "-p", hdfs_path],
                check=True,
                capture_output=True,
                text=True,
            )
            logging.info(f"HDFS 디렉토리 생성 완료: {hdfs_path}")
        else:
            logging.info(f"HDFS 디렉토리가 이미 존재합니다: {hdfs_path}")
    except subprocess.CalledProcessError as e:
        logging.error(f"HDFS 디렉토리 확인 또는 생성 오류: {e.stderr}")
        raise


def clear_hdfs_directory(hdfs_path):
    try:
        subprocess.run(
            ["hdfs", "dfs", "-rm", "-r", f"{hdfs_path}/*"],
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info(f"HDFS 디렉토리 초기화 완료: {hdfs_path}")
    except subprocess.CalledProcessError as e:
        if "No such file or directory" in e.stderr:
            logging.warning(f"HDFS 디렉토리가 비어 있음 또는 없음: {hdfs_path}")
        else:
            logging.error(f"HDFS 디렉토리 초기화 오류: {e.stderr}")
            raise


def load_and_merge_data(spark, input_dir):
    """
    HDFS에서 Parquet 파일을 읽고 병합하는 함수
    """
    try:
        logging.info(f"HDFS에서 데이터 로드 시작: {input_dir}")
        df = spark.read.parquet(input_dir)
        logging.info(f"데이터 로드 완료: {df.count()} rows")
        return df
    except Exception as e:
        logging.error(f"HDFS 데이터 로드 실패: {str(e)}")
        raise


def preprocess_data(df):
    """
    데이터 전처리 함수
    """
    logging.info("전처리 작업 시작")
    df = df.drop(
        "지번부번지",
        "건물부번지",
        "동정보",
        "층정보",
        "호정보",
        "건물명",
        "건물관리번호",
        "구우편번호",
        "법정동코드",
        "행정동코드",
        "지번코드",
        "대지구분코드",
        "도로명",
        "건물본번지",
        "지번본번지",
        "상가업소번호",
        "상권업종대분류코드",
        "상권업종중분류코드",
        "시도코드",
        "도로명코드",
    )
    logging.info("필요 없는 컬럼 제거 완료")

    df = df.fillna(
        {
            "지점명": "본점",
        }
    )
    df = df.dropna(subset=["상호명", "표준산업분류코드", "표준산업분류명"])
    logging.info("결측값 처리 완료")

    df = df.withColumn("경도", col("경도").cast("double"))
    df = df.withColumn("위도", col("위도").cast("double"))
    logging.info("데이터 타입 변환 완료")

    df = df.filter(
        (col("경도") >= -180)
        & (col("경도") <= 180)
        & (col("위도") >= -90)
        & (col("위도") <= 90)
    )
    logging.info("이상값 제거 완료")

    df = df.withColumn(
        "상권상태",
        when((col("위도") > 37.5) & (col("경도") > 126.9), lit("중심상권")).otherwise(
            lit("기타")
        ),
    )
    logging.info("상권 상태 분류 완료")

    return df


def main():
    spark = (
        SparkSession.builder.appName("PySpark Preprocessing")
        .config("spark.master", "yarn")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.hadoop.fs.defaultFS", "hdfs://sandbox-hdp.hortonworks.com:8020")
        .getOrCreate()
    )

    try:
        # HDFS에서 데이터 로드
        df = load_and_merge_data(spark, HDFS_INPUT_DIR)

        # 데이터 전처리
        processed_df = preprocess_data(df)

        # HDFS 디렉토리 준비
        ensure_hdfs_directory(HDFS_OUTPUT_DIR)
        clear_hdfs_directory(HDFS_OUTPUT_DIR)

        # HDFS에 전처리 데이터 저장
        processed_df.coalesce(1).write.mode("overwrite").option(
            "encoding", "UTF-8"
        ).parquet(HDFS_OUTPUT_DIR)
        logging.info(f"HDFS에 데이터 저장 완료: {HDFS_OUTPUT_DIR}")
    except Exception as e:
        logging.error(f"오류 발생: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
