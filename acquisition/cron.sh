# 2, 5, 8, 11월의 1일 0시 0분마다 작업 실행
(crontab -l 2>/dev/null; echo "0 0 1 2,5,8,11 * /opt/anaconda3/bin/python /Users/bk/Documents/code/commercial-analysis/acquisition/crawling.py") | crontab -