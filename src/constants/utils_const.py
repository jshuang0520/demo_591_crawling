# -*- coding: utf-8 -*-
from enum import Enum


class LogFormatConst(Enum):
    BASIC = '[%(asctime)s] - {%(lineno)d} - %(name)s - %(levelname)s - %(message)s'
    FORMAT_1 = '[%(asctime)s] - {%(pathname)s:%(lineno)d} - %(name)s - %(levelname)s - %(message)s'
    FORMAT_2 = '[%(asctime)s] - p%(process)s - {%(pathname)s:%(lineno)d} - %(name)s - %(levelname)s - %(message)s'


class ConfigFileTypeConst(Enum):
    YAML = 'yaml'
    INI = 'ini'
    JSON = 'json'
