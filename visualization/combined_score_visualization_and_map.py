import pandas as pd
import googlemaps
import folium
from folium.plugins import MarkerCluster
import matplotlib.pyplot as plt

# 데이터 로드
file_path = "C:/Users/82104/Downloads/res2.txt"  # 파일 경로

# 데이터 로드
data = pd.read_csv(file_path, sep='|', skiprows=2, skipinitialspace=True)

# 불필요한 첫 번째 행 삭제 및 열 정리
data_cleaned = data.drop(index=0).reset_index(drop=True)
data_cleaned = data_cleaned.loc[:, ~data_cleaned.columns.str.contains('^Unnamed')]

# "combined_score" 컬럼을 숫자로 변환
data_cleaned.columns = data_cleaned.columns.str.strip()  # 열 이름의 공백 제거
data_cleaned['combined_score'] = pd.to_numeric(data_cleaned['combined_score'], errors='coerce')

# 1. 최대 Combined Score 시각화 (막대 그래프)
max_combined_score_by_region = data_cleaned.groupby('시도명')['combined_score'].max().reset_index()

# 막대 그래프 생성
plt.figure(figsize=(10, 6))
plt.barh(max_combined_score_by_region['시도명'], max_combined_score_by_region['combined_score'], color='skyblue')
plt.xlabel('Maximum Combined Score')
plt.ylabel('Region (시도명)')
plt.title('Maximum Combined Score by Region')
plt.tight_layout()
plt.show()

# Google Maps API 키 설정
google_map_key = 'api'

# googlemaps 패키지로 maps 객체 생성
maps = googlemaps.Client(key=google_map_key)

# Folium 지도 초기화 (중앙 위치는 대한민국으로 설정)
m = folium.Map(location=[36.5, 127.5], zoom_start=7)

# MarkerCluster 객체 생성
marker_cluster = MarkerCluster().add_to(m)

# 시도별 최고 Combined Score에 대한 위도, 경도를 Google Maps API로 검색 후 마커 추가
for index, row in max_combined_score_by_region.iterrows():
    region = row['시도명']
    score = row['combined_score']
    
    # Google Maps API를 통해 위도, 경도 가져오기
    results = maps.geocode(region)
    
    if results:
        lat, lng = results[0]['geometry']['location']['lat'], results[0]['geometry']['location']['lng']
        
        # Folium 마커 추가
        folium.Marker(
            location=[lat, lng],
            popup=f'{region}: {score:.2f} Combined Score',
            tooltip=f'{region} 위치'
        ).add_to(marker_cluster)

# 지도 저장 (HTML 파일로 저장)
m.save('combined_score_map_with_google.html')

# 지도 표시
m
