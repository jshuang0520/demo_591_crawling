# -*- coding: utf-8 -*-
import multiprocessing
from multiprocessing import Process, Lock

# ================================================================================================================


# def sum_up_to(number):
#     return sum(range(1, number + 1))
#
#
# a_pool = multiprocessing.Pool()  # Create pool object
# result = a_pool.map(sum_up_to, range(10))  # Run `sum_up_to` 10 times simultaneously
#
# print(result)


# ================================================================================================================
#
#
# import time
#
#
# def sum_up_to(number):
#     start = time.time()
#     res = sum(range(1, number + 1))
#     end = time.time()
#     print(end - start)
#     return {'res': res, 'number': number}
#
#
# total_start = time.time()
#
# a_pool = multiprocessing.Pool()  # Create pool object
# result = a_pool.map(sum_up_to, range(10))  # Run `sum_up_to` 10 times simultaneously
# print(result)
#
# total_end = time.time()
# print(total_end-total_start)


# ================================================================================================================


# import time
# from src.crawler.crawler_util import Crawling
# from bs4 import BeautifulSoup
# import json
# import random
# import re
# import requests
# import sys
# from src.constants.crawler_constant import UserAgentConst
# from src.utility.utils import Logger
# import pprint
#
#
# pp = pprint.PrettyPrinter(indent=2)
# global_logger = Logger().get_logger('crawling')
#
#
# taipei_total_num = Crawling(global_logger).craw(city='taipei_city')['total']
#
#
# total_start = time.time()
#
# a_pool = multiprocessing.Pool()  # Create pool object
# result = a_pool.map(Crawling(global_logger).craw(city='taipei_city'), range(0, 300, 30))  # Run `sum_up_to` 10 times simultaneously
# print(result)
#
# total_end = time.time()
# print(total_end-total_start)


# ================================================================================================================


# from multiprocessing import Process
# import os
#
#
# def info(title):
#     print(title)
#     print('module name:', __name__)
#     print('parent process:', os.getppid())
#     print('process id:', os.getpid())
#
#
# def f(name):
#     info('function f')
#     print('hello', name)
#
#
# if __name__ == '__main__':
#     info('main line')
#     p = Process(target=f, args=('bob',))
#     p.start()
#     p.join()


# ================================================================================================================


# from multiprocessing import Process, Lock
#
#
# def f(l, i):
#     l.acquire()
#     try:
#         print('hello world', i)
#     finally:
#         l.release()
#
#
# if __name__ == '__main__':
#     import time
#     lock = Lock()
#
#     # multi process
#     start = time.time()
#     for num in range(100):
#         Process(target=f, args=(lock, num)).start()
#     end = time.time()
#     print('multi process:', end - start)
#
#     # usual loop
#     start = time.time()
#     for num in range(100):
#         print('hi', num)
#     end = time.time()
#     print('usual loop:', end - start)


# ================================================================================================================


# import multiprocessing
#
#
# def worker():
#     """worker function"""
#     print ('Worker')
#     return
#
#
# if __name__ == '__main__':
#     jobs = []
#     for i in range(5):
#         p = multiprocessing.Process(target=worker)
#         jobs.append(p)


# ================================================================================================================


from multiprocessing import Process, Pool
import os, time
from src.crawler.crawler_util import Crawling


def main_map(i):
    result = i * i
    return result


if __name__ == '__main__':
    inputs = range(0, 10, 3)  # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

    # 設定處理程序數量
    pool = Pool(4)

    # 運行多處理程序
    pool_outputs = pool.map(main_map, inputs)

    # 輸出執行結果
    print(pool_outputs)


# # ================================================================================================================
#
#
# import multiprocessing
# from itertools import product
#
#
# def merge_names(a, b='aaaaa'):
#     return '{} & {}'.format(a, b)
#
#
# if __name__ == '__main__':
#     names = ['Brown', 'Wilson', 'Bartlett', 'Rivera', 'Molloy', 'Opie']
#     names_2 = ['Brown', 'Wilson', 'Bartlett', 'Rivera', 'Molloy', 'Opie'][::-1]
#     print(names_2)
#     with multiprocessing.Pool(processes=3) as pool:
#         results = pool.starmap(merge_names, zip(names, names_2))
#     print(results)
#     print(list(zip(names, names_2)))
#
#     # with multiprocessing.Pool(processes=3) as pool:
#     #     results = pool.starmap(merge_names, names)
#     # print(results)


# ================================================================================================================


"""
reference:

How to use multiprocessing pool.map with multiple arguments?
https://stackoverflow.com/questions/5442910/how-to-use-multiprocessing-pool-map-with-multiple-arguments
"""

# import multiprocessing
# from itertools import product
#
#
# def merge_names(a, b='aaaaa'):
#     return '{} & {}'.format(a, b)
#
#
# if __name__ == '__main__':
#     names = ['Brown', 'Wilson', 'Bartlett', 'Rivera', 'Molloy', 'Opie']
#     names_2 = ['Brown', 'Wilson', 'Bartlett', 'Rivera', 'Molloy', 'Opie'][::-1]
#     print(names_2)
#     with multiprocessing.Pool(processes=3) as pool:
#         results = pool.starmap(merge_names, zip(names, names_2))
#     print(results)
#     print(list(zip(names, names_2)))
#
#     # with multiprocessing.Pool(processes=3) as pool:
#     #     results = pool.starmap(merge_names, names)
#     # print(results)



import time
from src.crawler.crawler_util import Crawling
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


taipei_total_num = Crawling(global_logger).craw(city='taipei_city')['total']

# # print(list(zip(taipei_city*, range(0, 10, 3))))
# print(len(range(0, 10, 3)), list(range(0, 10, 3)))
# print(['taipei_city'] * len(range(0, 10, 3)))

start = time.time()
# rng = range(0, taipei_total_num, 30)
rng = range(0, 70, 30)
with multiprocessing.Pool(processes=3) as pool:
    results = pool.starmap(Crawling(global_logger).craw, zip(['taipei_city']*len(rng), rng))
end = time.time()
print('time elapsed:', end - start)
# print(results)

# final_res = list()
# for res in results:
#     for r in res['data']:
#         final_res.append(r['id'])
# final_res = sorted(final_res)
# print("final_res:", final_res, len(final_res))
# list_set_final_res = sorted(list(set(final_res)))
# print("list(set(final_res)):", list_set_final_res, len(list_set_final_res))

# import collections
# print([item for item, count in collections.Counter(final_res).items() if count > 1])


# ================================================================================================================


def run():
    def f(x):
        return x*x
    p = Pool()
    return p.map(f, [1, 2, 3])


run()
