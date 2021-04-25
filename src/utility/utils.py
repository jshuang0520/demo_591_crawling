# -*- coding: utf-8 -*-
from dotenv import load_dotenv
import logging
import os
import sys
from src import SRC_PARENT_FOLDER
from src.constants.utils_const import LogFormatConst, ConfigFileTypeConst
import time
import yaml


class Logger:
    @property
    def log_format(self):
        return self._log_format

    @log_format.setter
    def log_format(self, log_format):
        self._log_format = log_format

    def __init__(self, log_format=None):
        if log_format is None:
            log_format = LogFormatConst.FORMAT_2.value
        self._log_format = log_format

    def get_logger(self, name, level=logging.INFO):
        logger = logging.getLogger(name)
        if logger.hasHandlers():
            logger.handlers.clear()
        logger.setLevel(level)
        formatter = logging.Formatter(self.log_format)
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(level)
        sh.setFormatter(formatter)
        logger.addHandler(sh)
        return logger


class ConfigParser:
    def __init__(self, logger):
        self.logger = logger

    @staticmethod
    def __parse_yaml_file(file):
        stream = open(file, 'r')
        doc = list(yaml.load_all(stream, Loader=yaml.FullLoader))
        config = doc[0]
        return config

    @staticmethod
    def __parse_ini_file(file):
        config = None  # TODO: implementation
        return config

    @staticmethod
    def __parse_json_file(file):
        config = None  # TODO: implementation
        return config

    def expand_vars(self, config, env_file_path):
        for k, v in config.items():
            if type(v) is dict:
                config[k] = self.expand_vars(config[k], env_file_path)
            else:
                config[k] = os.path.expandvars(v)
        return config

    def parse(self, config_type, file, env_file_path):
        # self.logger.info('read and parse configs from {}'.format(file))
        # Do not enable this logger because of parallel execution will parse configs many times
        if config_type == ConfigFileTypeConst.INI.value:
            config = self.__parse_ini_file(file)
        elif config_type == ConfigFileTypeConst.YAML.value:
            config = self.__parse_yaml_file(file)
        elif config_type == ConfigFileTypeConst.JSON.value:
            config = self.__parse_json_file(file)
        else:
            raise NotImplementedError()
        config = self.expand_vars(config, env_file_path)
        return config


def get_specific_file_extension(file_list):
    ext_list = ["csv", "json", "parquet"]
    ret = list()
    for f in file_list:
        ext = f.split('.')[-1]
        if ext in ext_list:
            if 'latest' not in f:
                ret.append(f)
    return ret


# def json_to_pandas_df(list_of_dict):
#     pd_ret = pd.DataFrame.from_dict(list_of_dict, orient='columns')
#     return pd_ret


def get_full_path(*path):
    return os.path.abspath(os.path.join(SRC_PARENT_FOLDER, *path))


def timeit(method):
    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()
        if 'log_time' in kw:
            name = kw.get('log_name', method.__name__.upper())
            kw['log_time'][name] = te - ts
        else:
            logger = Logger().get_logger('timeit')
            logger.info('%r  %.4f sec.' % (method.__name__, te - ts))
        return result

    return timed


def print_progress_bar(value, max_val):
    n_bar = 100
    j = value / max_val
    sys.stdout.write('\r')
    bar = '█' * int(n_bar * j)
    bar = bar + '-' * int(n_bar * (1 - j))

    sys.stdout.write(f"[{bar:{n_bar}s}] {int(100 * j)}% ")
    sys.stdout.flush()


def ensure_folder_path(path):
    if path.endswith(os.path.sep):
        return path
    else:
        path += os.path.sep
        return path


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def remove_options(parser, arg):
    for action in parser._actions:
        if (vars(action)['option_strings']
            and vars(action)['option_strings'][0] == arg) \
                or vars(action)['dest'] == arg:
            parser._remove_action(action)
    return parser


def set_env(logger, env_file_path, config_folder_name):
    config_path = get_full_path(config_folder_name)
    print("config_path:", config_path)
    proj_root = os.path.dirname(config_path)
    os.chdir(proj_root)
    env_file = os.path.abspath(env_file_path)
    # print("env_file path:", env_file)
    load_dotenv(env_file)
    config_parser = ConfigParser(logger)
    config_file = os.path.join(config_path, 'config.yml')
    # print("config_file path:", config_file)
    config = config_parser.parse('yaml', config_file, env_file)
    return config


def flatten(s):
    if s == list():
        return s
    if isinstance(s[0], list):
        return flatten(s[0]) + flatten(s[1:])
    return s[:1] + flatten(s[1:])


# def hashid_encode_to_str(hash_id, salt, min_length):
#     hashids = Hashids(salt=salt, min_length=min_length)
#     result = hashids.encode(hash_id)  # it's a tuple, e.g. (904708,)
#     return result
#
#
# def hashid_to_int(hash_id, salt, min_length):
#     hashids = Hashids(salt=salt, min_length=min_length)
#     result = hashids.decode(hash_id)  # it's a tuple, e.g. (904708,)
#     if len(result) == 1:
#         result = int(result[0])
#     else:
#         result = None
#     return result
