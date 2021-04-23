# -*- coding: utf-8 -*-
from datetime import datetime
import fire
import json
import multiprocessing.pool
import os
import pprint
import sys
import time
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
from src.crawler.crawler_util import Crawling
from src.database.mongo.mongodb import MongodbUtility
from src.utility.utils import Logger, set_env


pp = pprint.PrettyPrinter(indent=2)


class NoDaemonProcess(multiprocessing.Process):
    # make 'daemon' attribute always return False
    def _get_daemon(self):
        return False

    def _set_daemon(self, value):
        pass

    daemon = property(_get_daemon, _set_daemon)


class MyPool(multiprocessing.pool.Pool):
    """
    We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
    because the latter is only a wrapper function, not a proper class.
    """
    Process = NoDaemonProcess


class MainCommand:
    def __init__(self):
        __dir_path = os.path.dirname(os.path.realpath(__file__))
        self.logger = Logger().get_logger('main crawler')
        self.config = set_env(logger=self.logger,
                              env_file_path=__dir_path.split('mains')[0] + 'env_files/dev/.env',
                              config_folder_name='configs')
        self.mongodb_client = MongodbUtility(self.config, self.logger)
        self.db_conn = self.mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db

    def craw(self, city, pool_layer_1=10, pool_layer_2=50):

        data_total_num = Crawling(self.logger).craw_layer_1(
            city=city, fist_row=0, get_total=True)['total']
        self.logger.info('total number: {}'.format(data_total_num))
        ########################################################################################################
        # layer 1
        start_layer_1_ts = time.time()
        rng = range(0, data_total_num, 30)
        rng = range(0, 15, 30)  # TODO: for testing
        pool = MyPool(pool_layer_1)
        results = pool.starmap(Crawling(self.logger).craw_layer_1, zip([city]*len(rng), rng, [False]*len(rng)))
        pool.close()
        pool.join()

        results_1 = []
        for result in results:
            results_1.extend(result['data'])
        end_layer_1_ts = time.time()

        self.logger.info('results_1[0:5]: {results}; length of results_1: {l}'.format(results=results_1[0:5], l=len(results_1)))
        self.logger.info('time elapsed for layer 1: {}'.format(end_layer_1_ts - start_layer_1_ts))
        ########################################################################################################
        # layer 2
        start_layer_2_ts = time.time()
        pool = MyPool(pool_layer_2)  # TODO - result of trials: pool <=4 to solve Error: 'RecursionError('maximum recursion depth exceeded')'
        results_2 = pool.map(Crawling(self.logger).craw_layer_2, results_1)  # input layer 1 result
        pool.close()
        pool.join()

        end_layer_2_ts = time.time()

        self.logger.info('results_2[0:5]: {results}; length of results_2: {l}'.format(results=results_2[0:5], l=len(results_2)))
        self.logger.info('time elapsed for layer 2: {}'.format(end_layer_2_ts - start_layer_2_ts))
        return results_2

    def db_insert(self, city, pool_layer_1=10, pool_layer_2=50):
        # mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})

        self.logger.info('start crawling city: {}'.format(city))
        all_data = self.craw(city, pool_layer_1, pool_layer_2)
        """
        time elapsed layer 2: 1520.5253620147705 sec
        """
        unix_ts = int(datetime.now().timestamp())
        with open('{city}_{ts}.json'.format(city=city, ts=unix_ts), 'w', encoding='utf-8') as f:
            data = {'data': all_data}
            json.dump(data, f, ensure_ascii=False)
        # print('type(all_data), len(all_data):', type(all_data), len(all_data), all_data[0:10], type(all_data[0]))
        self.logger.info('end crawling city: {}'.format(city))
        self.logger.info('start create_collection')
        self.mongodb_client.create_collection(conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city))
        self.logger.info('start create_index')
        self.mongodb_client.create_index(conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city), idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
        # mongodb_client.create(conn_db=db_conn, coll_name='{city}_renting'.format(city=city), data_to_insert=all_data)  # all_data is a list of dict
        self.logger.info('start update {city}_renting'.format(city=city))
        self.mongodb_client.update(conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city), data_to_insert=all_data, unique_key='post_id')
        self.logger.info('start read taipei_city_renting')
        self.mongodb_client.read(conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city), query_criteria=None, projection=None)
        self.logger.info('--- done for taipei_city')


if __name__ == '__main__':
    fire.Fire(MainCommand)


#
#
# # global_logger.info('start crawling new taipei city')
# # new_taipei_data = main(city='new_taipei_city')
# # import json
# # with open('~/py_ds_nas/591_cathay_interview/demo_591_crawling/new_taipei_city_20210422T232400.json', 'w') as f:
# #     json.dump(new_taipei_data, f)
# # # global_logger.info('type(new_taipei_data), len(new_taipei_data):', type(new_taipei_data), len(new_taipei_data), new_taipei_data[0:10], type(new_taipei_data[0]))
# # global_logger.info('end crawling new taipei city')
# # global_logger.info('start create_collection')
# # mongodb_client.create_collection(conn_db=db_conn, coll_name='new_taipei_city_renting')
# # global_logger.info('start create_index')
# # mongodb_client.create_index(conn_db=db_conn, coll_name='new_taipei_city_renting', idx_col_list=['post_id', 'gender_request', 'city', 'phone', 'owner_identity', 'owner_last_name', 'owner_gender'])
# # # mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=new_taipei_data)  # taipei_data is a list of dict
# # global_logger.info('start update new_taipei_city_renting')
# # mongodb_client.update(conn_db=db_conn, coll_name='new_taipei_city_renting', data_to_insert=new_taipei_data, unique_key='post_id')
# # global_logger.info('start read new_taipei_city_renting')
# # mongodb_client.read(conn_db=db_conn, coll_name='new_taipei_city_renting', query_criteria=None, projection=None)


"""
reference

How to use multiprocessing pool.map with multiple arguments?
https://stackoverflow.com/questions/5442910/how-to-use-multiprocessing-pool-map-with-multiple-arguments

Python Process Pool non-daemonic?
https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
"""
