# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import json
import pprint
import random
import re
import requests
import sys
from src.constants.crawler_constant import UserAgentConst
from src.utility.utils import Logger


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


class Crawling:
    def __init__(self,  logger):
        self.logger = logger
        self.user_agent_list = eval(UserAgentConst.STR_LIST.value)

    # def retry(self, retry_count=3, **kwargs):
    #     for i in range(retry_count):
    #         self.logger.info('Retrying for {} time(s)...'.format(i))
    #         time.sleep(2)
    #         status = self.craw(**kwargs)['status']
    #         if status > 0:
    #             break

    def craw(self, city, fist_row=0):  # FIXME: add fist_row
        """
        reference: https://ithelp.ithome.com.tw/articles/10191506
        """
        city = str(city).lower()
        if city == 'taipei_city':
            urlJumpIp, region = 1, 1
        elif city == 'new_taipei_city':
            urlJumpIp, region = 3, 3
        else:
            self.logger.error('This city is not implemented yet, please select a city in list')
            sys.exit()

        try:
            url_1 = 'https://rent.591.com.tw/?kind=0&region={region}'.format(region=region)
            res = requests.Session()  # 設置 resuest session
            user_agent = random.choice(self.user_agent_list)  # 隨機選擇 user_agent
            # url_1 : for CSRF Token
            headers1 = {'User-Agent': user_agent}
            # 地區不是由 regionid 控制，而是由'urlJumpIp'，要去設定cookies : https://ithelp.ithome.com.tw/articles/10191506
            cookies = dict(urlJumpIp=str(urlJumpIp))  # https://blog.m157q.tw/posts/2018/01/06/use-cookie-with-urllib-in-python/
            resp = res.get(url_1, headers=headers1, cookies=cookies)
            soup = BeautifulSoup(resp.text, 'lxml')
            csrfToken = soup.find('meta', {'name': 'csrf-token'})['content']
            # self.logger.info('csrfToken: {}'.format(csrfToken))

            # for data
            url_2 = 'https://rent.591.com.tw/home/search/rsList?is_new_list=1&type=1&kind=0&searchtype=1&firstRow={fist_row}&region={region}'.format(
                fist_row=fist_row, region=region)  # FIXME: add fist_row
            headers2 = {'User-Agent': user_agent,
                        'X-CSRF-TOKEN': csrfToken,
                        }
            getlist = res.get(url_2, headers=headers2, cookies=cookies)
            data = json.loads(getlist.text.encode('utf-8'))  # ASCII to utf8
            # self.logger.info("data['data'].keys(): {}".format(data['data'].keys()))  # dict_keys(['topData', 'biddings', 'data', 'page'])
            total_num = int(list(set(re.findall(r'data-total=.[0-9]+', data['data']['page'])))[0].replace("'", '"').split('"')[1])
            # self.logger.info("total_num: {}".format(total_num))
            # self.logger.info("data['data']['data']:\n{}".format(pp.pformat(data['data']['data'])))
            self.logger.info("data['data']['data'][0]:\n{}".format(data['data']['data'][0]))

            # craw the detailed information e.g. cell phone, do etl to retrieve columns
            data = data['data']['data']  # list of dict
            for i in range(0, len(data)):
                detail_info = self.craw_detailed(post_id=data[i]['post_id'])
                res = {'nick_name': data[i]['nick_name']}
                data[i] = {**res, **detail_info}  # merge multiple dictionaries to replace the original dict data

            return_dict = {'data': data,
                           'total': total_num,  # FIXME: add total
                           'status': 1}
            self.logger.info("return_dict['data'][0]: {}".format(return_dict['data'][0]))

        except Exception as e:
            self.logger.error(e)
            return_dict = {'data': None,
                           'total': None,  # FIXME: add total
                           'status': -1}  # FIXME: alter it from 0 to -1
        return return_dict

    def craw_detailed(self, post_id):
        url = 'https://rent.591.com.tw/rent-detail-{post_id}.html'.format(post_id=post_id)
        res = requests.Session()  # set resuest session
        user_agent = random.choice(self.user_agent_list)  # choose a user_agent randomly
        headers1 = {'User-Agent': user_agent}
        resp = res.get(url, headers=headers1)
        soup = BeautifulSoup(resp.text, 'lxml')

        # detailInfo clearfix
        detail_info = soup.find('div', {'class': 'detailInfo clearfix'}).find('ul', {'class': 'attr'}).findAll('li')
        lot_size, story, floor, types, status = None, None, None, None, None
        for info in detail_info:
            if '坪數' in info.text:
                lot_size = info.text.split(':')[-1].strip()
            elif '樓層' in info.text:
                story = info.text.split(':')[-1].strip()
                floor = story.split('/')[0]
            elif '型態' in info.text:
                types = info.text.split(':')[-1].strip()
            elif '現況' in info.text:
                status = info.text.split(':')[-1].strip()
        # userInfo
        user_info = soup.find('div', {'class': 'userInfo'})
        # print('user_info:', user_info.find('div', {'style': 'margin-top: 13px;'}).text)
        phone = user_info.find('span', {'class': 'dialPhoneNum'})['data-value']

        res = {'lot_size': lot_size,
               'story': story,
               'floor': floor,
               'types': types,
               'status': status,
               'phone': phone,
               }

        return res


# FIXME: if we don't comment out the 'main' function, other codes import this will execute this loop below
# import time
# total_start = time.time()
# taipei_total_num = Crawling(global_logger).craw(city='taipei_city')['total']
# for i in range(0, taipei_total_num, 30):
#     start = time.time()
#     total = len(Crawling(global_logger).craw(city='taipei_city', fist_row=i)['data'])
#     print('total:', total)
#     end = time.time()
#     print(end - start)
# # print(loop_data)
# total_end = time.time()
# print(total_end-total_start)
# # Crawling(global_logger).craw('new_taipei_city')


# FIXME: add some trial results
"""
take a look at total:
[2021-04-16 10:48:44,344] - p81328 - {/Users/johnson.huang/py_ds/AIdea/591_koko/src/crawler/crawler_util.py:72} - crawling - INFO - total_num: 12259
total_time_elapsed: 475.82827591896057

take a look at one query:
total: 30
time_elapsed: 1.1301171779632568


---

multi-process:
total time elapsed 1: 148.16877484321594
total time elapsed 2: 116.61687183380127
"""
