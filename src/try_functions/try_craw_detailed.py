import requests
from bs4 import BeautifulSoup
import json
import random
import re
import requests
import sys
from src.constants.crawler_constant import UserAgentConst
from src.utility.utils import Logger
import pprint


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


class Crawling:
    def __init__(self,  logger):
        self.logger = logger
        self.user_agent_list = eval(UserAgentConst.STR_LIST.value)

    def craw_detailed(self, post_id):
        url = 'https://rent.591.com.tw/rent-detail-{post_id}.html'.format(post_id=post_id)
        # res = requests.Session()  # 設置 resuest session
        user_agent = random.choice(self.user_agent_list)  # 隨機選擇 user_agent
        # url_1 : for CSRF Token
        headers1 = {'User-Agent': user_agent}
        # 地區不是由 regionid控制，而是由'urlJumpIp'，要去設定cookies : https://ithelp.ithome.com.tw/articles/10191506
        resp = requests.get(url, headers=headers1)
        soup = BeautifulSoup(resp.text, 'lxml')

        # detailInfo clearfix
        detail_info = soup.find('div', {'class': 'detailInfo clearfix'}).find('ul', {'class': 'attr'}).findAll('li')
        print("detail_info:", detail_info)
        for info in detail_info:
            # print(info.text)
            if '坪數' in info.text:
                lot_size = info.text.split(':')[-1].strip()
                print('lot_size:', lot_size)
            elif '樓層' in info.text:
                story = info.text.split(':')[-1].strip()
                print('story:', story)
                floor = story.split('/')[0]
                print('floor:', floor)
            elif '型態' in info.text:
                types = info.text.split(':')[-1].strip()
                print('types:', types)
            elif '現況' in info.text:
                status = info.text.split(':')[-1].strip()
                print('status:', status)
        # userInfo
        user_info = soup.find('div', {'class': 'userInfo'})
        # print('user_info:', user_info.find('div', {'style': 'margin-top: 13px;'}).text)
        phone = user_info.find('span', {'class': 'dialPhoneNum'})['data-value']
        print('phone:', phone)

        res = {'lot_size': lot_size,
               'story': story,
               'floor': floor,
               'types': types,
               'status': status,
               'phone': phone,
               }
        return res

        # # for data
        # data = json.loads(soup.text.encode('utf-8'))  # ASCII to utf8
        # print('data:', data)


Crawling(global_logger).craw_detailed(post_id=10741776)
