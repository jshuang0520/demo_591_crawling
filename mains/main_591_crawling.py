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


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


taipei_total_num = Crawling(global_logger).craw(city='taipei_city')['total']
global_logger.info('total number: {}'.format(taipei_total_num))

# # print(list(zip(taipei_city*, range(0, 10, 3))))
# print(len(range(0, 10, 3)), list(range(0, 10, 3)))
# print(['taipei_city'] * len(range(0, 10, 3)))

start = time.time()
# rng = range(0, taipei_total_num, 30)
rng = range(0, 100, 30)
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



# # create
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
