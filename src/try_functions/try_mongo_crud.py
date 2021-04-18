# -*- coding: utf-8 -*-
import os
import pymongo
import sys
from src.constants.crawler_constant import UserAgentConst
from src.utility.utils import Logger


def connect(database, host='localhost', port=27017):
    connection = pymongo.MongoClient(host, port)
    db_connection = connection[database]
    print("connection.database_names():", connection.list_database_names())
    return db_connection


connect(database='test', host='localhost', port=27017)
