# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import json
import multiprocessing
import pprint
import random
import re
import requests
import sys
from src.constants.crawler_constant import UserAgentConst
from src.utility.utils import Logger
import time


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


class Crawling:
    def __init__(self,  logger):
        self.logger = logger
        # self.user_agent_list = eval(UserAgentConst.STR_LIST.value)

    # # # def retry(self, retry_count=3, **kwargs):
    # # #     for i in range(retry_count):
    # # #         self.logger.info('Retrying for {} time(s)...'.format(i))
    # # #         time.sleep(2)
    # # #         status = self.craw(**kwargs)['status']
    # # #         if status > 0:
    # # #             break
    #
    # def craw(self, city, fist_row=0):
    #     """
    #     reference: https://ithelp.ithome.com.tw/articles/10191506
    #     """
    #     # FIXME: Error: 'RecursionError('maximum recursion depth exceeded')'
    #     sleep_time = random.randint(2, 7)
    #     time.sleep(sleep_time)
    #
    #     city = str(city).lower()
    #     if city == 'taipei_city':
    #         urlJumpIp, region = 1, 1
    #     elif city == 'new_taipei_city':
    #         urlJumpIp, region = 3, 3
    #     else:
    #         self.logger.error('This city is not implemented yet, please select a city in list')
    #         sys.exit()
    #
    #     try:
    #         url_1 = 'https://rent.591.com.tw/?kind=0&region={region}'.format(region=region)
    #         res = requests.Session()  # 設置 resuest session
    #         # user_agent = random.choice(self.user_agent_list)  # randomly select user_agent
    #         user_agent = UserAgent().random  # randomly select user_agent
    #         # url_1 : for CSRF Token
    #         headers1 = {'User-Agent': user_agent}
    #         # 地區不是由 regionid 控制，而是由'urlJumpIp'，要去設定cookies : https://ithelp.ithome.com.tw/articles/10191506
    #         cookies = dict(urlJumpIp=str(urlJumpIp))  # https://blog.m157q.tw/posts/2018/01/06/use-cookie-with-urllib-in-python/
    #         resp = res.get(url_1, headers=headers1, cookies=cookies)
    #         soup = BeautifulSoup(resp.text, 'lxml')
    #         csrfToken = soup.find('meta', {'name': 'csrf-token'})['content']
    #         # self.logger.info('csrfToken: {}'.format(csrfToken))
    #
    #         # for data
    #         url_2 = 'https://rent.591.com.tw/home/search/rsList?is_new_list=1&type=1&kind=0&searchtype=1&firstRow={fist_row}&region={region}'.format(
    #             fist_row=fist_row, region=region)  # FIXME: add fist_row
    #         headers2 = {'User-Agent': user_agent,
    #                     'X-CSRF-TOKEN': csrfToken,
    #                     }
    #         getlist = res.get(url_2, headers=headers2, cookies=cookies)
    #         data = json.loads(getlist.text.encode('utf-8'))  # ASCII to utf8
    #         # self.logger.info("data['data'].keys(): {}".format(data['data'].keys()))  # dict_keys(['topData', 'biddings', 'data', 'page'])
    #         total_num = int(list(set(re.findall(r'data-total=.[0-9]+', data['data']['page'])))[0].replace("'", '"').split('"')[1])
    #         # self.logger.info("total_num: {}".format(total_num))
    #         # self.logger.info("data['data']['data']:\n{}".format(pp.pformat(data['data']['data'])))
    #         self.logger.info("data['data']['data'][0]:\n{}".format(data['data']['data'][0]))
    #
    #         # craw the detailed information e.g. cell phone, do etl to retrieve columns
    #         data = data['data']['data']  # list of dict
    #         # # # single process
    #         # for i in range(0, len(data)):
    #         #     detail_info = self.craw_detailed(post_id=data[i]['post_id'])
    #         #     res = {'nick_name': data[i]['nick_name']}
    #         #     data[i] = {**res, **detail_info}  # merge multiple dictionaries to replace the original dict data
    #         # # multi process
    #         post_id_list = [x['post_id'] for x in data]
    #         # print('post_id_list:', post_id_list)
    #         with multiprocessing.Pool(processes=5) as pool:
    #             results = pool.starmap(Crawling(self.logger).craw_detailed, zip(data, post_id_list))
    #             pool.close()
    #             pool.join()
    #             time.sleep(0.5)
    #         print('results:', results)
    #         # exit()
    #         print('len(data):', len(data))
    #         return_dict = {'data': data,
    #                        'total': total_num,
    #                        'status': 1}
    #         self.logger.info("return_dict['data'][0]: {}".format(return_dict['data'][0]))
    #
    #     except Exception as e:
    #         self.logger.error(e)
    #         return_dict = {'data': None,
    #                        'total': None,
    #                        'status': -1}
    #     return return_dict
    #
    # def craw_detailed(self, basic_data, post_id):
    #     try:
    #         # FIXME: Error: 'RecursionError('maximum recursion depth exceeded')'
    #         sleep_time = random.uniform(0.5, 3)  # random.uniform(0.5, 3)   random.randint(1, 3)
    #         time.sleep(sleep_time)
    #         # post_id = 10789981  # TODO: test case - there's no gender request
    #         url = 'https://rent.591.com.tw/rent-detail-{post_id}.html'.format(post_id=post_id)
    #         # res = requests.Session()  # set resuest session
    #         # user_agent = random.choice(self.user_agent_list)  # choose a user_agent randomly
    #         user_agent = UserAgent().random  # randomly select user_agent
    #         headers1 = {'User-Agent': user_agent}
    #         resp = requests.get(url, headers=headers1)
    #         soup = BeautifulSoup(resp.text, 'lxml')
    #
    #         # city
    #         city_info = soup.find('div', {'id': 'main'}).find('div', {'id': 'propNav'}).findAll('a')[2]  # FIXME: too roughly
    #
    #         # gender request
    #         gender_request_info = soup.find('div', {'class': 'detailBox clearfix'}) \
    #             .find('div', {'class': 'leftBox'}) \
    #             .find('ul', {'class': 'clearfix labelList labelList-1'})\
    #             .findAll('li', {'class': 'clearfix'})
    #         gender_request = None
    #         for x in gender_request_info:
    #             if '性別要求' in x.text:
    #                 gender_request = x.text.split('性別要求：')[-1]
    #         print('gender_request:', gender_request)
    #
    #         # detailInfo clearfix
    #         detail_info = soup.find('div', {'class': 'detailInfo clearfix'}).find('ul', {'class': 'attr'}).findAll('li')
    #         lot_size, story, floor, types, status = None, None, None, None, None
    #         for info in detail_info:
    #             if '坪數' in info.text:
    #                 lot_size = info.text.split(':')[-1].strip()
    #             elif '樓層' in info.text:
    #                 story = info.text.split(':')[-1].strip()
    #                 floor = story.split('/')[0]
    #             elif '型態' in info.text:
    #                 types = info.text.split(':')[-1].strip()
    #             elif '現況' in info.text:
    #                 status = info.text.split(':')[-1].strip()
    #         # userInfo
    #         user_info = soup.find('div', {'class': 'userInfo'})
    #         # print('user_info:', user_info.find('div', {'style': 'margin-top: 13px;'}).text)
    #         phone = user_info.find('span', {'class': 'dialPhoneNum'})['data-value']  # FIXME: find an exception- https://rent.591.com.tw/rent-detail-10780065.html
    #
    #         res = {'lot_size': lot_size,
    #                'story': story,
    #                'floor': floor,
    #                'types': types,
    #                'status': status,
    #                'phone': phone,
    #                }
    #
    #         nick_name = basic_data['nick_name']
    #         owner_id_info = [x.strip() for x in nick_name.split(' ')]
    #         if len(owner_id_info) > 1:
    #             owner_identity = owner_id_info[0]
    #             renter = owner_id_info[1]
    #             owner_last_name = self.lastname_gender(owner_id_info[1])[0]
    #             owner_gender = self.lastname_gender(owner_id_info[1])[1]
    #         else:
    #             owner_identity = owner_id_info[0]
    #             renter = None
    #             owner_last_name = None
    #             owner_gender = None
    #         basic_data = {'post_id': basic_data['post_id'], 'city': city_info,
    #                       'nick_name': nick_name, 'renter': renter,
    #                       'owner_identity': owner_identity, 'owner_last_name': owner_last_name, 'owner_gender': owner_gender,
    #                       }
    #         data = {**basic_data, **res}  # merge multiple dictionaries to replace the original dict data
    #     except Exception as e:
    #         self.logger.error(e)
    #         data = None
    #
    #     return data

    # ==================================================================================================

    def craw_layer_1(self, city, fist_row=0, get_total=False):
        """
        reference: https://ithelp.ithome.com.tw/articles/10191506
        --

        get total posts from data['data']['page']
        get 'post_id', 'nick_name' from data['data']['data']
        """
        # FIXME: Error: 'RecursionError('maximum recursion depth exceeded')'
        sleep_time = random.randint(1, 3)
        time.sleep(sleep_time)

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
            res = requests.Session()  # set request session
            user_agent = UserAgent().random  # randomly select user_agent
            # url_1: for CSRF Token
            headers1 = {'User-Agent': user_agent}
            # region is not controlled by 'regionid'. It's 'urlJumpIp', we set it from cookies settings:  https://ithelp.ithome.com.tw/articles/10191506
            cookies = dict(urlJumpIp=str(urlJumpIp))  # https://blog.m157q.tw/posts/2018/01/06/use-cookie-with-urllib-in-python/
            resp = res.get(url_1, headers=headers1, cookies=cookies)
            soup = BeautifulSoup(resp.text, 'lxml')
            csrfToken = soup.find('meta', {'name': 'csrf-token'})['content']
            # self.logger.info('csrfToken: {}'.format(csrfToken))

            # url_2: for data
            url_2 = 'https://rent.591.com.tw/home/search/rsList?is_new_list=1&type=1&kind=0&searchtype=1&firstRow={fist_row}&region={region}'.format(
                fist_row=fist_row, region=region)
            headers2 = {'User-Agent': user_agent,
                        'X-CSRF-TOKEN': csrfToken,
                        }
            getlist = res.get(url_2, headers=headers2, cookies=cookies)
            data = json.loads(getlist.text.encode('utf-8'))  # ASCII to utf8
            # self.logger.info("data['data'].keys(): {}".format(data['data'].keys()))  # dict_keys(['topData', 'biddings', 'data', 'page'])

            if get_total:
                total_num = int(list(set(re.findall(r'data-total=.[0-9]+', data['data']['page'])))[0].replace("'", '"').split('"')[1])
                return_dict = {'data': None,
                               'total': total_num,
                               'status': 1}
            else:
                # self.logger.info("total_num: {}".format(total_num))
                # self.logger.info("data['data']['data']:\n{}".format(pp.pformat(data['data']['data'])))
                # self.logger.info("data['data']['data'][0]:\n{}".format(data['data']['data'][0]))

                # get 'post_id', 'nick_name'
                list_of_dict_data = data['data']['data']  # list of dict
                list_of_dict_data = list({v['post_id']: v for v in list_of_dict_data}.values())   # unique
                data = list()
                for ldd in list_of_dict_data:
                    extract_data = {**{'post_id': ldd['post_id']}, **self.clean_nick_name({'nick_name': ldd['nick_name']})}
                    data.append(extract_data)
                # data = list(set(data))  # unique -> ERROR: use this would cause TypeError: object of type 'NoneType' has no len()
                return_dict = {'data': data,
                               'total': None,
                               'status': 1}
                self.logger.debug("return_dict['data'][0]: {}".format(return_dict['data'][0]))

        except Exception as e:
            self.logger.error(e)
            return_dict = {'data': {},
                           'total': None,
                           'status': -1}
        return return_dict

    def craw_layer_2(self, basic_data):
        try:
            post_id = basic_data['post_id']
            self.logger.info('post_id: {}'.format(post_id))
            # FIXME: Error: 'RecursionError('maximum recursion depth exceeded')'
            sleep_time = random.randint(1, 3)  # random.uniform(0.5, 3)   random.randint(1, 3)
            time.sleep(sleep_time)
            # # post_id = 10789981  # TODO: test case - there's no gender request
            url = 'https://rent.591.com.tw/rent-detail-{post_id}.html'.format(post_id=post_id)
            # res = requests.Session()  # set resuest session
            # user_agent = random.choice(self.user_agent_list)  # choose a user_agent randomly
            user_agent = UserAgent().random  # randomly select user_agent
            headers1 = {'User-Agent': user_agent}
            resp = requests.get(url, headers=headers1)
            soup = BeautifulSoup(resp.text, 'lxml')

            # city
            city_info = soup.find('div', {'id': 'main'}).find('div', {'id': 'propNav'}).findAll('a')[
                2]  # FIXME: too roughly
            city_info = city_info.text
            city_data = {'city': city_info}

            # gender request
            gender_request_info = soup.find('div', {'class': 'detailBox clearfix'}) \
                .find('div', {'class': 'leftBox'}) \
                .find('ul', {'class': 'clearfix labelList labelList-1'}) \
                .findAll('li', {'class': 'clearfix'})
            gender_request = None
            for x in gender_request_info:
                if '性別要求' in x.text:
                    gender_request = x.text.split('性別要求：')[-1]
            # print('gender_request:', gender_request)
            gender_request_data = {'gender_request': gender_request}

            # detailInfo clearfix
            detail_info = soup.find('div', {'class': 'detailInfo clearfix'}).find('ul', {'class': 'attr'}).findAll(
                'li')
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
            phone = user_info.find('span', {'class': 'dialPhoneNum'})[
                'data-value']  # FIXME: find an exception- https://rent.591.com.tw/rent-detail-10780065.html

            res = {'lot_size': lot_size,
                   'story': story,
                   'floor': floor,
                   'types': types,
                   'status': status,
                   'phone': phone,
                   }
            data = {**basic_data, **city_data, **res, **gender_request_data}  # merge multiple dictionaries to replace the original dict data
        except Exception as e:
            self.logger.error(e)
            data = None

        return data

    def clean_nick_name(self, nick_name_dict):
        try:
            nick_name = nick_name_dict['nick_name']
            owner_id_info = [x.strip() for x in nick_name.split(' ')]
            if len(owner_id_info) > 1:
                owner_identity = owner_id_info[0]
                renter = owner_id_info[1]
                owner_last_name = self.lastname_gender(owner_id_info[1])[0]
                owner_gender = self.lastname_gender(owner_id_info[1])[1]
            else:
                owner_identity = owner_id_info[0]
                renter = None
                owner_last_name = None
                owner_gender = None
            res = {'nick_name': nick_name, 'renter': renter,
                   'owner_identity': owner_identity, 'owner_last_name': owner_last_name,
                   'owner_gender': owner_gender,
                   }
        except Exception as e:
            self.logger.error(e)
            res = {'nick_name': None, 'renter': None,
                   'owner_identity': None, 'owner_last_name': None,
                   'owner_gender': None,
                   }
        data = {**nick_name_dict, **res}
        return data

    @staticmethod
    def lastname_gender(seller):
        try:
            if "先生" in seller:
                delimiters = "先生"
                nick_name_gender_local = "男"
                nick_name_lastName_local = re.split(delimiters, seller)[0]  # 取姓氏的部分
            elif any(x in seller for x in ["小姐", "太太", "媽媽", "女士", "阿姨"]):
                delimiters = "小姐|太太|媽媽|女士|阿姨"
                nick_name_gender_local = "女"
                nick_name_lastName_local = re.split(delimiters, seller)[0]  # 取姓氏的部分
            else:
                nick_name_lastName_local = seller
                nick_name_gender_local = None  # "未顯示"
            return nick_name_lastName_local, nick_name_gender_local
        except Exception as e:
            print(e)


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
