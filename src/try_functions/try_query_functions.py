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
import os
import sys
import time
from flask import request, jsonify, Flask
from flask_restplus import fields, Api, Resource, Namespace
sys.path.insert(0, os.path.abspath('..'))
sys.path.insert(0, os.path.abspath('.'))
from src.utility.utils import Logger, set_env
from src.database.mongo.mongodb import MongodbUtility, ApiQuery


global_logger = Logger().get_logger('try_query')
dir_path = os.path.dirname(os.path.realpath(__file__))
global_config = set_env(logger=global_logger,
                        env_file_path=dir_path.split('src/')[0] + 'env_files/dev/.env',
                        config_folder_name='configs')
# mongodb_client = MongodbUtility(global_config, global_logger)
query_handler = ApiQuery(global_config, global_logger)

query_handler.query_renter_gender(city='taipei_city', gender='男')
