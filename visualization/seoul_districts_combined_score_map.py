import pandas as pd
import folium
import googlemaps

# 1. Google Maps API 설정
google_map_key = 'api'
maps = googlemaps.Client(key=google_map_key)

# 2. 데이터 로드
file_path =  "C:/Users/82104/Downloads/res.txt"  # 파일 경로를 로컬 환경에 맞게 수정
data = pd.read_csv(file_path, delimiter=',', names=[
    "시도명", "행정동명", "상권업종중분류명", "상권업종소분류명", "count",
    "middle_category_count", "subcategory_ratio", "weighted_middle_category",
    "combined_score"
], skiprows=1)

# 데이터 클린업
data = data.dropna()  # 결측값 제거
data['combined_score'] = data['combined_score'].str.extract(r'([0-9.]+)').astype(float)  # 문자열 정리 및 변환
data['subcategory_ratio'] = data['subcategory_ratio'].str.extract(r'([0-9.]+)').astype(float)  # 문자열 정리 및 변환

# 3. 상위 Combined Score 계산
top_n = 10
top_combined_score = data.sort_values(by='combined_score', ascending=False).head(top_n)

# 4. 위도와 경도 가져오기
def get_lat_lng(location):
    results = maps.geocode(location)
    if results:
        geometry = results[0]['geometry']['location']
        return geometry['lat'], geometry['lng']
    else:
        return None, None

top_combined_score['lat_lng'] = top_combined_score['행정동명'].apply(lambda x: get_lat_lng(f"서울특별시 {x}"))

# 5. Folium 지도 시각화
seoul_map = folium.Map(location=[37.5665, 126.9780], zoom_start=12)

# 데이터의 위도, 경도와 Combined Score를 지도에 추가
for _, row in top_combined_score.iterrows():
    lat, lng = row['lat_lng']
    if lat and lng:
        folium.Marker(
            location=[lat, lng],
            popup=f"{row['행정동명']}: {row['combined_score']}",
            tooltip=row['행정동명']
        ).add_to(seoul_map)

# 6. 지도 보기
seoul_map
