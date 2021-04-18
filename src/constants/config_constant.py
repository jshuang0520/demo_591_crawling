# -*- coding: utf-8 -*-
from enum import Enum


class ConfigConstant(Enum):
    SETTINGS = 'settings'
    CRAWLER_STORAGE = 'crawler_storage'
    DATABASE = 'database'
    MONGODB = 'mongodb'
    URI = 'uri'
    HOST = 'host'
    PORT = 'port'
    USERNAME = 'username'
    PASSWORD = 'password'
    DFLT_CONN_DB = 'dflt_conn_db'
    DFLT_CONN_COLLECTION = 'dflt_conn_collection'
