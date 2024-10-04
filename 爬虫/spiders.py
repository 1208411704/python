import requests
import csv
import os
from pymysql import connect
from lxml import etree
import re
import json
import  pandas as pd
from sqlalchemy import  create_engine

engine =create_engine('mysql+pymysql://root:zhang030806@localhost:3306/dbm')
class Spider(object):
    def __init__(self):
        self.spiderUrl = 'https://m.douban.com/rexxar/api/v2/movie/recommend?'
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'Referer': 'https://m.douban.com',
            'Origin': 'https://m.douban.com',
            'Accept-Language': 'zh-CN,zh;q=0.9'
        }
        self.page = 0  # 初始化页码属性

    def init(self):
        # 定义CSV文件路径
        file_path = os.path.join(os.path.dirname(__file__), 'tempData.csv')

        # 如果文件不存在，则创建它
        if not os.path.exists(file_path):
            with open(file_path, 'w', newline='') as writer_f:
                writer = csv.writer(writer_f)
                writer.writerow(
                    ['title', 'value', 'year','card_subtitle', 'large', 'items', 'rating', 'types', 'country', 'lang', 'moveiTime', 'comment_len', 'comment', 'pic',
                    'star_count', 'value', 'tags',  'type', 'uri', 'vendor_icons',
                      'directors'])

        # 如果 spiderPage.txt 文件不存在，则创建它
        if not os.path.exists('./spiderPage.txt'):
            with open('./spiderPage.txt', 'w', encoding='utf8') as f:
                f.write('0\n')

        # 连接MySQL数据库并创建表（如果不存在）
        try:
            conn = connect(host='localhost', user='root', password='zhang030806', database='dbm', port=3306,
                           charset='utf8mb4')
            cursor = conn.cursor()
            sql = '''   
                     CREATE TABLE IF NOT EXISTS movie (
                        id INT PRIMARY KEY AUTO_INCREMENT,
                        items VARCHAR(255),
                        comment VARCHAR(255),
                        pic VARCHAR(255),
                        large VARCHAR(255),
                        rating VARCHAR(255),
                        star_count VARCHAR(255),
                        value VARCHAR(255),
                        tags VARCHAR(255),
                        title VARCHAR(255),
                        type VARCHAR(255),
                        uri VARCHAR(255),
                        vendor_icons VARCHAR(255),
                        year VARCHAR(255),
                        card_subtitle VARCHAR(255),
                        directors VARCHAR(255),
                        types VARCHAR(255),
                        country VARCHAR(255),
                        lang VARCHAR(255),
                        moveiTime VARCHAR(255),
                        comment_len VARCHAR(255),
                        starts VARCHAR(255)
                     )
                   '''
            cursor.execute(sql)
            conn.commit()
            print("数据库表创建成功。")
        except Exception as e:
            print(f"连接数据库或创建表时出错: {e}")



    def spiderMain(self):
        # 获取当前页码
        self.page = int(self.get_page())


        while True:
            params = {
                'count': 20,
                'start': self.page * 20,
                'playable': 'true',
            }
            print(f"正在爬取第{self.page}页")

            try:
                # 发送请求并获取响应
                print(f"请求URL: {self.spiderUrl}")
                print(f"请求参数: {params}")

                response = requests.get(self.spiderUrl, headers=self.headers, params=params, verify=False)

                if response.status_code == 200:
                    respJson = response.json().get('items', [])
                    if not respJson:  # 如果没有更多数据，退出循环
                        break

                    for index, movieData in enumerate(respJson):
                        print(f'正在爬取第 {index} 条')

                        resultData = []
                        # 提取电影数据中的各种数据点
                        title = movieData.get('title', '')
                        resultData.append(title if isinstance(title, str) else ','.join(title))

                        rating = movieData.get('rating', {})
                        resultData.append(rating.get('value', ''))

                        year = movieData.get('year', '')
                        resultData.append(year if isinstance(year, str) else ','.join(year))

                        card_subtitle = movieData.get('card_subtitle', '')
                        resultData.append(card_subtitle if isinstance(card_subtitle, str) else ','.join(card_subtitle))

                        pic = movieData.get('pic', {})
                        resultData.append(pic.get('large', '') if isinstance(pic, dict) else pic)

                        uri = movieData.get('uri', '')
                        cleaned_uri = uri.replace('douban://', '').replace('/movie/', '/subject/') if uri else ''
                        resultData.append('https://movie.' + cleaned_uri)

                        # 请求电影详情
                        if uri:
                            full_url = 'https://movie.' + cleaned_uri
                            print(f"请求的 URL: {full_url}")

                            try:
                                respDetaHTML = requests.get(full_url, headers=self.headers)
                                respDetaHTMLXpath = etree.HTML(respDetaHTML.text)

                                # 提取电影详情
                                types = [i.text for i in
                                         respDetaHTMLXpath.xpath('//div[@id="info"]/span[@property="v:genre"]')]
                                resultData.append(','.join(types))

                                textInfo = respDetaHTMLXpath.xpath('//div[@id="info"]/text()')
                                texts = [i.strip() for i in textInfo if i.strip() and i.strip() != '/']
                                resultData.append(','.join(texts[0].split(sep='/')))
                                resultData.append(','.join(texts[1].split(sep='/')))

                                date_info = respDetaHTMLXpath.xpath(
                                    '//div[@id="info"]/span[@property="v:initialReleaseDate"]/@content')
                                resultData.append(date_info[0].split('(')[0].strip() if date_info else '')

                                resultData.append(
                                    respDetaHTMLXpath.xpath('//div[@id="info"]/span[@property="v:runtime"]/@content')[
                                        0])

                                # 评论数量
                                comments_text = respDetaHTMLXpath.xpath(
                                    '//a[@href="https://movie.douban.com/subject/26752088/comments?status=P"]/text()')
                                if comments_text:
                                    comment_count = re.search(r'\d+', comments_text[0]).group()
                                    resultData.append(comment_count)
                                else:
                                    resultData.append('')

                                # 星级评分
                                starts = [i.xpath('./span[@class="rating_per"]/text()')[0] for i in
                                          respDetaHTMLXpath.xpath(
                                              '//div[@id="interest_sectl"]//div[@class="ratings-on-weight"]/div[@class="item"]')]
                                resultData.append(','.join(starts))

                                # 摘要
                                resultData.append(respDetaHTMLXpath.xpath(
                                    '//div[@id="link-report-intra"]/span[@property="v:summary"]/text()'))

                                # 评论
                                comments = []
                                commentsList = respDetaHTMLXpath.xpath('//div[@id="hot-comments"]/div')
                                for i in commentsList:
                                    user = i.xpath('.//h3/span[@class="comment-info"]/a/text()')
                                    user = user[0] if user else ''
                                    start = i.xpath('.//h3/span[@class="comment-info"]/span[2]/@class')
                                    start = re.search(r'\d+', start[0]).group() if start else ''
                                    time = i.xpath('.//h3/span[@class="comment-info"]/span[3]/@title')
                                    time = time[0] if time else ''
                                    content = i.xpath('.//span[@class="short"]/text()')
                                    content = content[0] if content else ''
                                    comments.append({'user': user, 'start': start, 'time': time, 'content': content})

                                resultData.append(json.dumps(comments))

                                # 预告片链接
                                try:
                                    movieUrl = respDetaHTMLXpath.xpath('//li[@class="label-trailer"]/a/@href')
                                    if movieUrl:
                                        movieUrl = movieUrl[0]
                                        movieHTML = requests.get(movieUrl, headers=self.headers)
                                        movieHTMLXpath = etree.HTML(movieHTML.text)

                                        video_url = movieHTMLXpath.xpath('//video/source/@src')[0]
                                        if video_url:
                                            print(video_url)  # 打印预告片视频链接
                                        else:
                                            print("未找到视频链接")
                                    else:
                                        print("未找到预告片链接")
                                except Exception as e:
                                    print(f"请求过程中出错: {e}")

                            except Exception as e:
                                print(f"请求电影详情时出错: {e}")
                        self.save_to_csv(resultData)
                        self.page += 1
                        self.set_page(self.page)

                else:
                    print(f"请求失败，状态码: {response.status_code}")
                    break

            except Exception as e:
                print(f"请求过程中出错: {e}")
                break

    def save_to_csv(self, rowData):
        with open('./tempData.csv', 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(rowData)

    def clear_csv(self):
        df = pd.read_csv('./tempData.csv')
        df.dropna(inplace=True)
        df.drop_duplicates()
        self.save_to_sql(df)

    def save_to_sql(self,df):
        pd.read_csv('./tempData.csv')
        df.to_sql('movie',con=engine,index=False,if_exists='append')


    def get_page(self):
        with open('./spiderPage.txt', 'r') as r_f:
            return r_f.readlines()[-1].strip()

    def set_page(self, newPage):
        with open('./spiderPage.txt', 'a') as w_f:
            w_f.write(str(newPage) + '\n')
if __name__ == '__main__':
    spiderObj = Spider()
    spiderObj.init()
    spiderObj.spiderMain()
    # spiderObj.clear_csv()