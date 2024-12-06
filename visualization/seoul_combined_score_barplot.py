import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

# 1. 데이터 로드
file_path = "C:/Users/82104/Downloads/res.txt"  # 파일 경로를 로컬 환경에 맞게 수정
data = pd.read_csv(file_path, delimiter=',', names=[
    "시도명", "행정동명", "상권업종중분류명", "상권업종소분류명", "count",
    "middle_category_count", "subcategory_ratio", "weighted_middle_category",
    "combined_score"
], skiprows=1)

# 데이터 클린업
data = data.dropna()  # 결측값 제거
data['combined_score'] = data['combined_score'].str.extract(r'([0-9.]+)').astype(float)  # 문자열 정리 및 변환
data['subcategory_ratio'] = data['subcategory_ratio'].str.extract(r'([0-9.]+)').astype(float)  # 문자열 정리 및 변환

# 2. 상위 Combined Score 계산
top_n = 10
top_combined_score = data.sort_values(by='combined_score', ascending=False).head(top_n)

# 3. 구분별 비율 계산
subcategory_ratio_avg = data.groupby('상권업종중분류명')['subcategory_ratio'].mean().sort_values(ascending=False)

# 4. 한글 폰트 설정
font_path = 'C:/Windows/Fonts/malgun.ttf'  # '맑은 고딕' 경로
font_prop = fm.FontProperties(fname=font_path)
plt.rcParams['font.family'] = font_prop.get_name()
plt.rcParams['axes.unicode_minus'] = False  # 마이너스 기호 깨짐 방지

# 5. 시각화
# (1) 행정동별 Combined Score 시각화
plt.figure(figsize=(12, 6))
plt.bar(top_combined_score['행정동명'], top_combined_score['combined_score'])
plt.title('Top Combined Scores by 행정동', fontsize=14)
plt.xlabel('행정동명', fontsize=12)
plt.ylabel('Combined Score', fontsize=12)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
