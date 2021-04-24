# -*- coding: utf-8 -*-
import os
from src.utility.utils import Logger, set_env
from src.database.mongo.mongodb import MongodbUtility
import pprint
from src.crawler.crawler_util import Crawling
from src.utility.utils import Logger
import time
import multiprocessing.pool


pp = pprint.PrettyPrinter(indent=2)
global_logger = Logger().get_logger('crawling')


dir_path = os.path.dirname(os.path.realpath(__file__))
global_config = set_env(logger=global_logger,
                        env_file_path=dir_path.split('src')[0] + 'env_files/dev/.env',
                        config_folder_name='configs')
mongodb_client = MongodbUtility(global_config, global_logger)
db_conn = mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db

# # mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})
#
#
# # taipei_data = main(city='taipei_city')
# # # print('type(taipei_data), len(taipei_data):', type(taipei_data), len(taipei_data), taipei_data[0:10], type(taipei_data[0]))
# # mongodb_client.create_collection(conn_db=db_conn, coll_name='taipei_city_renting')
# # mongodb_client.create_index(conn_db=db_conn, coll_name='taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
# # # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=taipei_data)  # taipei_data is a list of dict
# # mongodb_client.update(conn_db=db_conn, coll_name='taipei_city_renting', data_to_insert=taipei_data, unique_key='post_id')
# mongodb_client.read(conn_db=db_conn, coll_name='taipei_city_renting', query_criteria=None, projection=None)
#
#
# # new_taipei_data = main(city='new_taipei_city')
# # # print('type(new_taipei_data), len(new_taipei_data):', type(new_taipei_data), len(new_taipei_data), new_taipei_data[0:10], type(new_taipei_data[0]))
# # mongodb_client.create_collection(conn_db=db_conn, coll_name='new_taipei_city_renting')
# # mongodb_client.create_index(conn_db=db_conn, coll_name='new_taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
# # # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=new_taipei_data)  # taipei_data is a list of dict
# # mongodb_client.update(conn_db=db_conn, coll_name='new_taipei_city_renting', data_to_insert=new_taipei_data, unique_key='post_id')
# mongodb_client.read(conn_db=db_conn, coll_name='new_taipei_city_renting', query_criteria=None, projection=None)


from bson.json_util import dumps
# res = db_conn.taipei_city_renting.find({'post_id': 10700982})
# print([eval(dumps(r)) for r in res])
# doc_count = db_conn.taipei_city_renting.count_documents({})
# print(doc_count)
# doc_count = db_conn.new_taipei_city_renting.count_documents({})
# print(doc_count)


# # db_conn.new_taipei_city_renting.drop()
# res = db_conn.new_taipei_city_renting.find({"post_idn": {"$ne" : None}}).limit(2)
# print([eval(dumps(r)) for r in res])


res = db_conn.taipei_city_renting.find({"post_id": {"$ne" : None}}).limit(1)
print([eval(dumps(r)) for r in res])
"""
[
{'_id': {'$oid': '60803fd03d3a69615bf53dae'}, 
'post_id': 1531998, 
'nick_name': '代理人 陳先生', 'renter': '陳先生', 'owner_identity': '代理人', 'owner_last_name': '陳', 'owner_gender': '男', 
'city': '台北市', 
'lot_size': '7坪', 
'story': '3F/4F', 'floor': '3F', 
'types': '公寓', 
'status': '獨立套房', 
'phone': '0933-668-596', 
'gender_request': '女生'}, {'_id': {'$oid': '6080c4683d3a69615bf54572'}
]
"""
print("--")
res = db_conn.taipei_city_renting.find({"$and": [{"post_id": {'$ne': None}},
                                                 {"gender_request": {"$regex" : ".*男.*"}},
                                                 # {"city": {"$eq": "台北市"}}
                                                 ]
                                        }
                                       ).limit(1)
original = [eval(dumps(r)) for r in res]
print('original:', original)

# print("-- test projection")
# res = db_conn.taipei_city_renting.find({"gender_request": {"$regex" : ".*男.*"}}, {"post_id": {'$ne': None}}).limit(1)
# """
# Expression $ne takes exactly 2 arguments. 1 were passed in.
# """
# test_projection = [eval(dumps(r)) for r in res]
# print('test_projection:', test_projection)


print("--")
res = db_conn.taipei_city_renting.find({"phone": {"$eq": "0933-668-596"}}).limit(1)
print([eval(dumps(r)) for r in res])


print("--")
res = db_conn.taipei_city_renting.find({"owner_identity": {"$ne": "屋主"}}).limit(1)
print([r for r in res])  # eval(dumps(r)) - NameError: name 'null' is not defined


print("--")
res = db_conn.taipei_city_renting.find({"$and": [{"post_id": {'$ne': None}},
                                                 {"owner_gender": {"$eq": "女"}},
                                                 {"owner_last_name": {"$eq": "吳"}},
                                                 ]
                                        }
                                       ).limit(1)
print([eval(dumps(r)) for r in res])
