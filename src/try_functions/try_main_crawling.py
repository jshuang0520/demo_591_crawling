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

# mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})


# taipei_data = main(city='taipei_city')
# # print('type(taipei_data), len(taipei_data):', type(taipei_data), len(taipei_data), taipei_data[0:10], type(taipei_data[0]))
# mongodb_client.create_collection(conn_db=db_conn, coll_name='taipei_city_renting')
# mongodb_client.create_index(conn_db=db_conn, coll_name='taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
# # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=taipei_data)  # taipei_data is a list of dict
# mongodb_client.update(conn_db=db_conn, coll_name='taipei_city_renting', data_to_insert=taipei_data, unique_key='post_id')
mongodb_client.read(conn_db=db_conn, coll_name='taipei_city_renting', query_criteria=None, projection=None)


# new_taipei_data = main(city='new_taipei_city')
# # print('type(new_taipei_data), len(new_taipei_data):', type(new_taipei_data), len(new_taipei_data), new_taipei_data[0:10], type(new_taipei_data[0]))
# mongodb_client.create_collection(conn_db=db_conn, coll_name='new_taipei_city_renting')
# mongodb_client.create_index(conn_db=db_conn, coll_name='new_taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
# # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=new_taipei_data)  # taipei_data is a list of dict
# mongodb_client.update(conn_db=db_conn, coll_name='new_taipei_city_renting', data_to_insert=new_taipei_data, unique_key='post_id')
mongodb_client.read(conn_db=db_conn, coll_name='new_taipei_city_renting', query_criteria=None, projection=None)