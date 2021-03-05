import requests

url = 'http://www.kfc.com.cn/kfccda/ashx/GetStoreList.ashx?op=keyword'

kw = input('enter a word:')

headers = {
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.190 Safari/537.36'
}

param = {
    'cname':'' ,
    'pid':'',
    'keyword':kw,
    'pageIndex': '1',
    'pageSize': '10',
}
response = requests.post(url=url,params=param,headers = headers)

page_text = response.text

with open('./test.txt','w',encoding= 'utf-8',) as fp:
    fp.write(page_text)
print('done')

