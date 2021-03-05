import requests
from bs4 import BeautifulSoup
url = 'http://www.shicimingju.com/book/sangguoyanyi.html'

# 对首页的页面进行数据爬取
headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'
}
url = 'http://wwww.shicimingju.com/book/sangguoyanyi.html'
page_text = requests.get(url=url,headers=headers)

# 解析章节的标提和详情页的url
# 1实例化BeautifulSoup对象
soup = BeautifulSoup(page_text,'lxml')

# 解析章节标提