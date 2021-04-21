# -*- coding: utf-8 -*-
import os
from src.utility.utils import Logger, set_env
from src.database.mongo.mongodb import MongodbUtility


# # single process
# if __name__ == '__main__':
#     global_logger = Logger().get_logger('main crawler')
#     dir_path = os.path.dirname(os.path.realpath(__file__))
#     # print("dir_path:", dir_path)
#     global_config = set_env(logger=global_logger,
#                             env_file_path=dir_path.split('mains')[0] + 'env_files/dev/.env',
#                             config_folder_name='configs')
#     mongodb_client = MongodbUtility(global_config, global_logger)
#     db_conn = mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db
#     coll_conn = db_conn[mongodb_client.dflt_conn_collection]
#     non_private_methods = [method_name for method_name in dir(mongodb_client)
#                            if callable(getattr(mongodb_client, method_name)) and '__' not in method_name]
#     global_logger.info("object non_private_methods: {}".format(non_private_methods))
#
#     # data collect
#     from src.crawler.crawler_util import Crawling
#     test_data = Crawling(global_logger).craw('taipei_city')['data']
#     print('type(test_data):', type(test_data))
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=test_data)
#
#     data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#                                query_criteria={}, projection={})
#     data = data['data']
#     print('read data:', data)
#
#     print(0)
#
#     # # create
#     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 1}])
#     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 2}])
#     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 3}])
#     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 4}])
#     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 5}])
#     #
#     # print(1)
#     #
#     # # read
#     # data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#     #                            query_criteria={'item': {'$gte': 3}}, projection={'item': 'journal'})
#     # data = data['data']
#     # print('read data:', data)
#     #
#     # print(2)
#     #
#     # # update
#     # mongodb_client.update(conn_db=db_conn, coll_name='my_collection',
#     #                       update_filter={'item': {'$gte': 3}},
#     #                       update_action={'$set': {'item': 10, 'status': "good"}
#     #                                      }
#     #                       )
#     #
#     # data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#     #                            query_criteria={'item': {'$gte': 3}}, projection={'item': 'journal'})
#     # data = data['data']
#     # print('read data:', data)
#     #
#     # print(3)
#     #
#     # # # delete
#     # # mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})
#     #
#     # data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#     #                            query_criteria={'item': {'$lte': 3}}, projection={'item': 'journal'})
#     # data = data['data']
#     # print('read data:', data)
#     #
#     # print(4)
#     #
#     # # for item in db_conn.my_collection.find():
#     # #     print("item:", item)
#     #
#     # # db_conn.create_collection('test_create_coll2')
#     # # print(db_conn.my_collection.find({}, {'item': 'journal'}))


# =====================================================================================================================
# multi process
import multiprocessing
import pprint
from src.crawler.crawler_util import Crawling
from src.utility.utils import Logger
import time
import multiprocessing.pool


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


taipei_total_num = Crawling(global_logger).craw_layer_1(city='taipei_city', fist_row=0, get_total=True)['total']  # FIXME: total number encountered an error after multiprocess
global_logger.info('total number: {}'.format(taipei_total_num))


# # # print(list(zip(taipei_city*, range(0, 10, 3))))
# # print(len(range(0, 10, 3)), list(range(0, 10, 3)))
# # print(['taipei_city'] * len(range(0, 10, 3)))
#
# """
# How to use multiprocessing pool.map with multiple arguments?
# https://stackoverflow.com/questions/5442910/how-to-use-multiprocessing-pool-map-with-multiple-arguments
# """
# start = time.time()
# # rng = range(0, taipei_total_num, 30)
# rng = range(0, 100, 30)
# print('list(rng):', list(rng))
# with multiprocessing.Pool(processes=3) as pool:
#     results = pool.starmap(Crawling(global_logger).craw, zip(['taipei_city']*len(rng), rng))
#     pool.close()
#     pool.join()
# end = time.time()
# print('time elapsed:', end - start)
# # print(results)
#
# # final_res = list()
# # for res in results:
# #     for r in res['data']:
# #         final_res.append(r['id'])
# # final_res = sorted(final_res)
# # print("final_res:", final_res, len(final_res))
# # list_set_final_res = sorted(list(set(final_res)))
# # print("list(set(final_res)):", list_set_final_res, len(list_set_final_res))
#
# # import collections
# # print([item for item, count in collections.Counter(final_res).items() if count > 1])
#
#
#
# # # create
# #     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 1}])
# #     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 2}])
# #     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 3}])
# #     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 4}])
# #     # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 5}])
# #     #
# #     # print(1)
# #     #
# #     # # read
# #     # data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
# #     #                            query_criteria={'item': {'$gte': 3}}, projection={'item': 'journal'})
# #     # data = data['data']
# #     # print('read data:', data)


"""
reference

Python Process Pool non-daemonic?
https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
"""


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.


class MyPool(multiprocessing.pool.Pool):
    Process = NoDaemonProcess


def main(city):
    ########################################################################################################
    # layer 1
    start = time.time()
    rng = range(0, taipei_total_num, 30)
    # rng = range(0, 15, 30)  # TODO: testing size
    print('list(rng):', list(rng))
    pool = MyPool(5)
    results = pool.starmap(Crawling(global_logger).craw_layer_1, zip([city]*len(rng), rng, [False]*len(rng)))
    pool.close()
    pool.join()

    results_1 = []
    for res in results:
        results_1.extend(res['data'])
    end = time.time()

    global_logger.info('results_1[0:10]: {results}; length of results_1: {l}'.format(results=results_1[0:10], l=len(results_1)))

    cnt = 0
    for res in results:
        cnt += len(res['data'])
    print('cnt:', cnt)

    print('time elapsed layer 1:', end - start)

    # return results

    ########################################################################################################
    # layer 2
    start = time.time()

    # multi process
    pool = MyPool(4)  # TODO - result of trials: pool <=4 to solve Error: 'RecursionError('maximum recursion depth exceeded')'
    results_2 = pool.map(Crawling(global_logger).craw_layer_2, results_1)  # input layer 1 result
    pool.close()
    pool.join()

    # # single process
    # results_2 = list()
    # for res_1 in results_1:
    #     results_2.append(Crawling(global_logger).craw_layer_2(basic_data=res_1))

    end = time.time()

    global_logger.info('results_2[0:10]: {results}; length of results_2: {l}'.format(results=results_2[0:10], l=len(results_2)))

    # print('time elapsed layer 2:', end - start)
    global_logger.info('time elapsed layer 2: {t}'.format(t=end-start))

    # global_logger.info('results_1[0:10]: {results}; length of results_1: {l}'.format(results=results_1[0:10], l=len(results_1)))
    cnt = 0
    for res in results:
        cnt += len(res['data'])
    print('cnt:', cnt)

    print('time elapsed layer 2:', end - start)
    return results_2


taipei_data = main(city='taipei_city')
new_taipei_data = main(city='new_taipei_city')

# print('type(taipei_data), len(taipei_data):', type(taipei_data), len(taipei_data), taipei_data[0:10], type(taipei_data[0]))
# print('type(new_taipei_data), len(new_taipei_data):', type(new_taipei_data), len(new_taipei_data), new_taipei_data[0:10], type(new_taipei_data[0]))


dir_path = os.path.dirname(os.path.realpath(__file__))
global_config = set_env(logger=global_logger,
                        env_file_path=dir_path.split('mains')[0] + 'env_files/dev/.env',
                        config_folder_name='configs')
mongodb_client = MongodbUtility(global_config, global_logger)
db_conn = mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db

# mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})

mongodb_client.create_collection(conn_db=db_conn, coll_name='taipei_city_renting')
mongodb_client.create_collection(conn_db=db_conn, coll_name='new_taipei_city_renting')

mongodb_client.create_index(conn_db=db_conn, coll_name='taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
mongodb_client.create_index(conn_db=db_conn, coll_name='new_taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])

# mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=taipei_data)  # taipei_data is a list of dict
# mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=new_taipei_data)  # taipei_data is a list of dict


mongodb_client.update(conn_db=db_conn, coll_name='taipei_city_renting', data_to_insert=taipei_data, unique_key='post_id')
mongodb_client.update(conn_db=db_conn, coll_name='new_taipei_city_renting', data_to_insert=new_taipei_data, unique_key='post_id')

mongodb_client.read(conn_db=db_conn, coll_name='taipei_city_renting', query_criteria=None, projection=None)
mongodb_client.read(conn_db=db_conn, coll_name='new_taipei_city_renting', query_criteria=None, projection=None)




# import os
# from src.utility.utils import Logger, set_env
# from src.database.mongo.mongodb import MongodbUtility
# global_logger = Logger().get_logger('main crawler')
# dir_path = os.path.dirname(os.path.realpath(__file__))
# # print("dir_path:", dir_path)
# global_config = set_env(logger=global_logger,
#                         env_file_path=dir_path.split('mains')[0] + 'env_files/dev/.env',
#                         config_folder_name='configs')
# mongodb_client = MongodbUtility(global_config, global_logger)
# db_conn = mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db
# import pymongo
# client = pymongo.MongoClient(port=27017, host='localhost')
# res = client.test.taipei_city_renting.find({})
# print('taipei city data:', [x for x in res])
#
#
# import json
# from bson import ObjectId
# from uuid import UUID
# class JSONEncoder(json.JSONEncoder):
#     def default(self, o):
#         if isinstance(o, ObjectId):
#             return str(o)
#         elif isinstance(o, UUID):
#             return str(o)
#         return json.JSONEncoder.default(self, o)
#
#
# from bson.json_util import dumps
# res = db_conn['taipei_city_renting'].find({})
# res = [json.loads(json.dumps(r, cls=JSONEncoder)) for r in res]
# print('taipei city data:', res)
