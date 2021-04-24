# -*- coding: utf-8 -*-
import fire
# https://google.github.io/python-fire/guide/


########################################################


class CommandLine:

    @staticmethod
    def hi(city):
        return city


if __name__ == '__main__':
    fire.Fire(CommandLine)
    """$ python src/try_functions/try_argv.py hi --city=taipei_city"""


########################################################


def hihi(a, b):
    return a + b


def hello(word):
    return word


if __name__ == '__main__':

    fire.Fire()
    """python src/try_functions/try_argv.py hihi --a=1 --b=2"""
