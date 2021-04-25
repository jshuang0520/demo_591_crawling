# -*- coding: utf-8 -*-
from bson import ObjectId
# from bson.json_util import dumps
import inspect
import json
import os
import pprint
import pymongo
from pymongo import ReplaceOne  # InsertOne, DeleteOne
from pymongo.errors import BulkWriteError
from src.constants.config_constant import ConfigConstant
# from src.utility.utils import Logger, set_env
import time
from typing import List
# from uuid import UUID

pp = pprint.PrettyPrinter(indent=2)


class JSONEncoder(json.JSONEncoder):
    """
    make ObjectId, UUID json serializable
    --

    reference
    https://stackoverflow.com/questions/16586180/typeerror-objectid-is-not-json-serializable
    """
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        # elif isinstance(o, UUID):
        #     return str(o)
        return json.JSONEncoder.default(self, o)


class MongodbUtility:
    """
    reference:

    --
    CRUD api Implementation - https://iq.opengenus.org/mongodb-crud-operations-with-python/#4crudimplementation
    CRUD function - https://stackabuse.com/integrating-mongodb-with-python-using-pymongo/

    --
    official docs
    MONGODB MANUAL - https://docs.mongodb.com/manual/crud/
    Tutorial - https://pymongo.readthedocs.io/en/stable/tutorial.html
    """

    def __init__(self, config, logger):
        self.logger = logger
        database = config[ConfigConstant.SETTINGS.value][ConfigConstant.CRAWLER_STORAGE.value]
        self.config = config[ConfigConstant.DATABASE.value][database]
        # self.logger.info('self.config: \n{}'.format(pp.pformat(self.config)))  # uncomment this when debugging
        self.host = self.config[ConfigConstant.HOST.value]
        self.port = int(self.config[ConfigConstant.PORT.value])
        self.username = self.config[ConfigConstant.USERNAME.value]
        self.password = self.config[ConfigConstant.PASSWORD.value]
        self.dflt_conn_db = self.config[ConfigConstant.DFLT_CONN_DB.value]
        self.dflt_conn_collection = self.config[ConfigConstant.DFLT_CONN_COLLECTION.value]
        # # self.charset =
        # # self.conn_pool_size = int()
        # self._client = pymongo.MongoClient(self.host, self.port)
        # self._db = self._client[dbname]

    ###########################################
    #
    # database manipulation
    #
    ###########################################

    # def __create_db(self, mongo_client, database=None):
    #     if database:
    #         db_list = mongo_client.list_database_names()
    #         if database in db_list:
    #             self.logger.warn('Database "{}" is already existed.'.format(database))
    #         else:
    #             a = mongo_client[database]
    #     else:
    #         self.logger.warn('Please specify a database to create.')

    def db_connect(self, database=None):
        if not database:
            database = self.dflt_conn_db
        connection = pymongo.MongoClient(self.host, self.port)
        self.logger.info("mongodb in [host: {host}], [port: {port}] includes databases: {dbs}".format(
            host=self.host, port=self.port, dbs=connection.list_database_names()))
        db_connection = connection[database]
        self.logger.info('connecting to db: {}'.format(database))
        self.logger.info("database '{db}' includes collections: {coll}".format(db=database,
                                                                               coll=db_connection.list_collection_names()))
        return db_connection

    # def exec_cmd(self, fstring_cmd):
    #     try:
    #         self.logger.info('cmd: {}'.format(fstring_cmd))
    #         exec(f'%s' % fstring_cmd)
    #         return 1
    #     except Exception as e:
    #         self.logger.error(e)
    #         return -1
    #
    # def retry(self, fstring_cmd, retry_count=3):
    #     for i in range(retry_count):
    #         self.logger.info('Retrying cmd {cmd} for the {nth} time(s)'.format(cmd=fstring_cmd, nth=i+1))
    #         time.sleep(0.5)
    #         status = self.exec_cmd(fstring_cmd)
    #         if status > 0:
    #             break
    #
    # def create_collection(self, db, coll):
    #     """
    #     param: db: conn db name
    #     param: coll: conn collection name
    #     """
    #     f_string = f'{db}[{coll}]'.format(db=db, coll=coll)
    #     return self.exec_cmd(f_string)
    #
    # def insert_one(self, db, coll, data):
    #     import json
    #     """
    #     param: db: conn db name
    #     param: coll: conn collection name
    #     param: data: dictionary data to be inserted
    #     """
    #     self.logger.info(db)
    #     self.logger.info(coll)
    #     self.logger.info(data)
    #     self.logger.info(type(data))
    #     self.logger.info(type(json.dumps(data)))
    #     f_string = f'''{db}[{coll}].insert_one('''.format(db=db, coll=coll) + json.dumps(data) + f''')'''
    #     return self.exec_cmd(f_string)

    ###########################################
    #
    # collection manipulation
    #
    ###########################################

    def create_collection(self, conn_db, coll_name):
        """
        param: conn_db: db connection
        param: coll_name: collection name
        """
        try:
            res = conn_db[coll_name]
            output = {'status': int(1)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1)}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    def __drop_collection(self, conn_db, coll_name):
        """
        truncate collection

        param: conn_db: db connection
        param: coll_name: collection name
        """
        try:
            conn_db[coll_name].drop()
            output = {'status': int(1)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1)}
        self.logger.info('status: {}'.format(output['status']))
        return output

    def show_collections(self, conn_db, must_include=None):
        try:
            coll_names = conn_db.collection_names()
            if must_include:
                print('must_include:', must_include)
                coll_names = [x for x in coll_names if str(must_include) in x]  # include the key words
            output = {'status': int(1), 'data': coll_names}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1), 'data': None}
        self.logger.info('status: {}'.format(output['status']))
        return output

    def create_index(self, conn_db, coll_name, idx_col_list):
        """
        create index
        https://stackoverflow.com/questions/50301130/how-we-can-create-an-index-on-mongodb
        https://docs.mongodb.com/manual/indexes/#index-types
        --
        e.g.
        conn_db[coll_name].create_index([("post_id", pymongo.ASCENDING), ("city", pymongo.ASCENDING)])
        """
        try:
            if idx_col_list:
                conn_db[coll_name].create_index([(idx, pymongo.ASCENDING) for idx in idx_col_list])
            else:
                self.logger.warning('There are no column names as inputs.')
            output = {'status': int(1)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1)}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    def create(self, conn_db, coll_name, data_to_insert: List[dict]):
        """
        param: conn_db: db connection
        param: coll: collection name
        param: data_to_insert: <list of dictionary> data_to_insert to be inserted
        """
        try:
            res = conn_db[coll_name].insert_many(data_to_insert)
            output = {'status': int(1), 'document_ids': str(res.inserted_ids)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1), 'document_ids': None}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    def read(self, conn_db, coll_name, query_criteria=None, projection=None):
        """
        param: conn_db: db connection
        param: coll: collection name
        param: query_criteria: <dict> filter condition with operators
               reference - https://www.javatpoint.com/mongodb-query-and-projection-operator

               e.g. $gt (greater than >), $lt (less than <), $eq (=), $gte (>=), $ne (not equal !=),
                    $in (in a list), $nin (not in a list),

                    $or
                    { $or: [ { <exp_1> }, { <exp_2> }, ... , { <exp_n> } ] }
                    db.books.find ( { $or: [ { quantity: { $lt: 200 } }, { price: 500 } ] } )

                    $and
                    { $and: [ { <exp1> }, { <exp2> }, ....]}
                    db.books.find ( { $and: [ { price: { $ne: 500 } }, { price: { $exists: true } } ] } )

                    $not
                    { field: { $not: { <operator-expression> } } }
                    db.books.find ( { price: { $not: { $gt: 200 } } } )


        param: projection: <dict> projection
        """
        try:
            db_query_start = time.time()
            self.logger.info('query_criteria: {query_criteria}, projection: {projection}'.format(
                query_criteria=query_criteria, projection=projection))
            if not query_criteria and not projection:  # read all data in this collection
                res = conn_db[coll_name].find({})
            elif query_criteria:
                res = conn_db[coll_name].find(query_criteria).limit(100)  # FIXME: test limit
            else:
                res = conn_db[coll_name].find(query_criteria, projection)
            db_query_end = time.time()
            time_elapsed = float("{:.6f}".format(db_query_end - db_query_start))
            self.logger.info('elapsed time in db query: {}'.format(time_elapsed))

            # Error: 'Object of type ObjectId is not JSON serializable'
            # better way to make ObjectId, UUID json serializable - https://stackoverflow.com/questions/16586180/typeerror-objectid-is-not-json-serializable
            # res = [json.loads(json.dumps(r, cls=JSONEncoder, ensure_ascii=False).encode('utf8')) for r in res]  # to ensure
            if res:
                res = [json.loads(json.dumps(r, cls=JSONEncoder)) for r in res]
            else:
                res = json.loads(json.dumps(None, cls=JSONEncoder))
            # res = res[0]  # FIXME: for test

            # # Error:  name 'null' is not defined
            # from bson.json_util import dumps
            # # use bson.json_util to turn bson 'ObjectId' into json, for jsonify api output
            # res = [eval(dumps(r)) for r in res]  # res = [r for r in res] would cause issues in api return

            output = {'status': int(1), 'data': res, 'time_elapsed': time_elapsed}
            self.logger.info(
                'output["data"][0]: {d}; length of output["data"]: {l}'.format(d=res[0], l=len(res)))  # print first row
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1), 'data': None, 'time_elapsed': int(0)}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    def update(self, conn_db, coll_name, data_to_insert, unique_key='post_id'):
        """
        bulk / batch update

        param: conn_db: db connection
        param: coll_name: collection name
        param: data_to_insert: <list of dictionary> data_to_insert to be inserted
        param: unique_key: default setting: column 'post_id', we see this column as a Primary Key
        --

        bulk write
        https://pymongo.readthedocs.io/en/stable/examples/bulk.html

        count
        https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html
        --

        e.g.
        test_update_crawled_data = eval(
            "[{'post_id': 3757630, 'nick_name': '代理人 王先生 hihi', 'renter': '王先生 hello'}, {'post_id': 3757631, 'nick_name': '代理人 王小姐 hihi', 'renter': '王小姐 hello'}]")
        requests = [
            ReplaceOne(filter={"post_id": doc["post_id"]}, replacement=doc, upsert=True) for doc in
            test_update_crawled_data
            # ReplaceOne({'j': 2}, {'i': 5}),
            # InsertOne({'_id': 4}),  # Violates the unique key constraint on _id.
            # DeleteOne({'i': 5})
        ]
        try:
            conn_db.my_collection.bulk_write(requests)
        except BulkWriteError as bwe:
            print(bwe.details)
        """

        def chunks(lst, n):
            """
            Yield successive n-sized chunks from lst.
            --
            https://stackoverflow.com/questions/312443/how-do-you-split-a-list-into-evenly-sized-chunks
            """
            for i in range(0, len(lst), n):
                yield lst[i:i + n]
        n_elements = 1000  # num of elements in a chunck
        data_to_insert_in_n_chunks = list(chunks(data_to_insert, n_elements))

        for chunk in data_to_insert_in_n_chunks:
            try:
                requests = [
                    ReplaceOne(filter={unique_key: doc[unique_key]}, replacement=doc, upsert=True) for doc in chunk
                    if doc is not None
                ]  # remove None, or bulk write would be in exception thus no data were inserted
                result = conn_db[coll_name].bulk_write(requests)
                self.logger.info(
                    '{func} - status: {status}, inserted_cnt: {ins_cnt}, modified_cnt: {mod_cnt}'.format(
                        func=inspect.getframeinfo(inspect.currentframe()).function,
                        status=int(1),
                        ins_cnt=result.inserted_count, mod_cnt=result.modified_count
                    )
                )
            except BulkWriteError as bwe:
                self.logger.error(
                    '{func} - status: {status}, error message: {e}'.format(
                        func=inspect.getframeinfo(inspect.currentframe()).function,
                        status=int(-1),
                        e=bwe.details)
                )

    def update_many(self, conn_db, coll_name, update_filter=None, update_action=None):
        """
        param: conn_db: db connection
        param: coll: collection name
        param: update_filter: <dict>
        param: update_action: <dict>
        """
        try:
            if update_filter:
                conn_db[coll_name].update_many(update_filter, update_action)
            else:
                conn_db[coll_name].update_many(update_action)
            output = {'status': int(1)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1)}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    def delete(self, conn_db, coll_name, delete_criteria=None):
        """
        param: conn_db: db connection
        param: coll: collection name
        param: delete_criteria: <dict>
        """
        try:
            if delete_criteria is None:
                self.logger.warning('please specify a condition to delete data')
            elif delete_criteria == {}:
                self.logger.warning('deleting all data in this collection...')
                conn_db[coll_name].delete_many(delete_criteria)
            else:
                conn_db[coll_name].delete_many(delete_criteria)
            output = {'status': int(1)}
        except Exception as e:
            self.logger.error(e)
            output = {'status': int(-1)}
        self.logger.info('{func} - status: {status}'.format(func=inspect.getframeinfo(inspect.currentframe()).function,
                                                            status=output['status']))
        return output

    # db_conn.my_collection.insert_many([
    #     {"item": "journal"},
    # ])

    # db_conn.my_collection.delete_many(
    #     {'item': 'journal'}
    # )


class ApiQuery(MongodbUtility):
    def __init__(self, config, logger):
        super().__init__(config, logger)
        self.mongodb_client = MongodbUtility(config, logger)
        self.db_conn = self.mongodb_client.db_connect(database=self.mongodb_client.dflt_conn_db)  # default db conn

    def query_renter_gender(self, city, gender):
        """
        param: city: Enum(['taipei_city'], ['new_taipei_city'])
        param: gender: Enum(['男', '女'])
        """
        # list_of_dict = list()
        list_of_dict = self.mongodb_client.read(
            conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city),
            query_criteria={"$and": [{"post_id": {'$ne': None}},
                                     {"gender_request": {"$regex": ".*{gender}.*".format(gender=gender)}},
                                     # {"city": {"$eq": "台北市"}},
                                     ]
                            },
            projection=None
        )['data']
        return list_of_dict

    def query_owner_phone(self, phone):
        """
        param: phone: <str>, sample format: '0933-668-596'
        """
        list_of_dict = list()
        # so our collection naming design is: ends_with '_renting'
        collections = self.mongodb_client.show_collections(
            conn_db=self.mongodb_client.db_connect(self.mongodb_client.dflt_conn_db), must_include='_renting')['data']
        for coll in collections:
            data = self.mongodb_client.read(
                conn_db=self.db_conn, coll_name=coll,
                query_criteria={"$and": [{"post_id": {'$ne': None}},
                                         {"phone": {"$eq": str(phone)}},
                                         # {"city": {"$eq": "台北市"}},
                                         ]
                                },
                projection=None
            )['data']

            if data:
                list_of_dict.extend(data)
            else:
                pass

        # # TODO: this part might failed to use multi-porcessing due to passing an db_connection_object
        # import multiprocessing
        # query_criteria = {"$and": [{"post_id": {'$ne': None}},
        #                            {"phone": {"$eq": str(phone)}},
        #                            ]
        #                   }
        # projection = None
        #
        # with multiprocessing.Pool(processes=3) as pool:
        #     # zip(conn_db, coll_name, query_criteria, projection)
        #     list_of_dict = pool.starmap(self.mongodb_client.read, zip([self.db_conn]*len(collections),
        #                                                          collections,
        #                                                          [query_criteria]*len(collections),
        #                                                          [projection]*len(collections)
        #                                                          )
        #                            )
        #     pool.close()
        #     pool.join()
        #     """
        #     TypeError: can't pickle _thread.lock objects
        #     """
        #
        # data = [x['data'] for x in list_of_dict if x['data'] is not None]

        return list_of_dict

    def query_owner_identity(self, positive_id_lst=None, negative_id_lst=None):
        """
        param: city
        param: owner_identity
        """
        list_of_dict = list()
        if negative_id_lst:
            # so our collection naming design is: ends_with '_renting'
            collections = self.mongodb_client.show_collections(
                conn_db=self.mongodb_client.db_connect(self.mongodb_client.dflt_conn_db), must_include='_renting')['data']
            for coll in collections:
                data = self.mongodb_client.read(
                    conn_db=self.db_conn, coll_name=coll,
                    query_criteria={"$and": [{"post_id": {'$ne': None}},
                                             {"owner_identity": {"$nin": negative_id_lst}},
                                             # {"city": {"$eq": "台北市"}},
                                             ]
                                    },
                    projection=None
                )['data']

                if data:
                    list_of_dict.extend(data)
                else:
                    pass

        # list_of_dict = list()
        # if negative_id_lst:
        #     list_of_dict = self.mongodb_client.read(
        #         conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city),
        #         query_criteria={"$and": [{"post_id": {'$ne': None}},
        #                                  {"owner_identity": {"$nin": negative_id_lst}},
        #                                  # {"city": {"$eq": "台北市"}},
        #                                  ]
        #                         },
        #         projection=None
        #     )['data']
        return list_of_dict

    def query_owner_gender_last_name(self, city, owner_gender, owner_last_name):
        """
        param: city: Enum(['taipei_city'], ['new_taipei_city'])
        param: gender: Enum(['男', '女'])
        param: last_name
        """
        # list_of_dict = list()
        list_of_dict = self.mongodb_client.read(
            conn_db=self.db_conn, coll_name='{city}_renting'.format(city=city),
            query_criteria={"$and": [{"post_id": {'$ne': None}},
                                     {"owner_gender": {"$eq": str(owner_gender)}},
                                     {"owner_last_name": {"$eq": str(owner_last_name)}},
                                     # {"city": {"$eq": "台北市"}},
                                     ]
                            },
            projection=None
        )['data']
        return list_of_dict


# # test main flow
# if __name__ == '__main__':
#     from src.utility.utils import Logger
#     from src.utility.utils import set_env
#     import os
#
#     global_logger = Logger().get_logger('mongodb')
#     dir_path = os.path.dirname(os.path.realpath(__file__))
#     global_config = set_env(logger=global_logger,
#                             env_file_path=dir_path.split('src/')[0] + 'env_files/dev/.env',
#                             config_folder_name='configs')
#     mongodb_client = MongodbUtility(global_config, global_logger)
#     db_conn = mongodb_client.db_connect(database='test')  # mongodb_client.dflt_conn_db
#     coll_conn = db_conn[mongodb_client.dflt_conn_collection]
#     non_private_methods = [method_name for method_name in dir(mongodb_client)
#                            if callable(getattr(mongodb_client, method_name)) and '__' not in method_name]
#     global_logger.info("object non_private_methods: {}".format(non_private_methods))
#
#     # create
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 1}])
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 2}])
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 3}])
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 4}])
#     mongodb_client.create(conn_db=db_conn, coll_name='my_collection', data_to_insert=[{"item": 5}])
#
#     print(1)
#
#     # read
#     data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#                                query_criteria={'item': {'$gte': 3}}, projection={'item': 'journal'})
#     data = data['data']
#     print('read data:', data)
#
#     print(2)
#
#     # update
#     mongodb_client.update(conn_db=db_conn, coll_name='my_collection',
#                           update_filter={'item': {'$gte': 3}},
#                           update_action={'$set': {'item': 10, 'status': "good"}
#                                          }
#                           )
#
#     data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#                                query_criteria={'item': {'$gte': 3}}, projection={'item': 'journal'})
#     data = data['data']
#     print('read data:', data)
#
#     print(3)
#
#     # # delete
#     # mongodb_client.delete(conn_db=db_conn, coll_name='my_collection', delete_criteria={})
#
#     data = mongodb_client.read(conn_db=db_conn, coll_name='my_collection',
#                                query_criteria={'item': {'$lte': 3}}, projection={'item': 'journal'})
#     data = data['data']
#     print('read data:', data)
#
#     print(4)
#
#     # for item in db_conn.my_collection.find():
#     #     print("item:", item)
#
#     # db_conn.create_collection('test_create_coll2')
#     # print(db_conn.my_collection.find({}, {'item': 'journal'}))
