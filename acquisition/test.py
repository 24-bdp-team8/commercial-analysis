import pandas as pd
import time

start_time = time.time()
df1 = pd.read_parquet("data/gyeonggi.parquet")
end_time = time.time()
print(f"parquet 읽기 시간: {end_time - start_time}")

start_time = time.time()
df2 = pd.read_csv("downloads/소상공인시장진흥공단_상가(상권)정보_경기_202409.csv", low_memory=False)
end_time = time.time()
print(f"csv 읽기 시간: {end_time - start_time}")
