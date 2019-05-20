# coding:utf-8
__author__ = ''

import logging.config
import logging
import StringIO
import copy
import inspect
import sys


EMPTY_DICT = {}


# 日志加载器，根据配置加载
def load_dict_config(dict_config, arg_info=None):
    """
    dict_config : python 27 的dictConfig定义
    将字典转换为 ini 文件
    """
    # format the handler args
    if arg_info:
        for difinition in dict_config.get('handlers', {}).itervalues():
            difinition['args'] = difinition['args'] % arg_info

    dict_config_func = getattr(logging.config, 'dictConfig', None)
    if dict_config_func:
        dcfg = copy.deepcopy(dict_config)
        dcfg['version'] = 1
        for lcfg in dcfg.get('loggers', {}).values():
            if isinstance(lcfg['handlers'], basestring):
                lcfg['handlers'] = [s.strip() for s in lcfg['handlers'].split(',')]
        rcfg = dcfg.get('loggers', {}).get('root', None)
        if rcfg:
            dcfg['root'] = rcfg
        for hcfg in dcfg.get('handlers', {}).values():
            args = hcfg.pop('args', '()')
            args = eval(args)
            klass = hcfg['class']
            try:
                klass = eval(klass, vars(logging))
            except (AttributeError, NameError):
                klass = logging.config._resolve(klass)
            init_func = getattr(klass, '__init__')
            init_args = inspect.getargspec(init_func).args[1:]
            for i in xrange(len(args)):
                hcfg[init_args[i]] = args[i]
        dict_config_func(dcfg)
    else:
        ini_cont = dict2ini(dict_config)
        logging.config.fileConfig(ini_cont)


def dict2ini(dict_cfg):
    ini_cont = StringIO.StringIO()
    for topic, cont in dict_cfg.items():
        ini_cont.write('[%s]\r\nkeys=' % topic)
        definitions = {}
        for name, definition in cont.items():
            if 'loggers' == topic:
                keys = name.split(r'.')
            else:
                keys = [name]
            # 记录定义
            for key in keys:
                definitions[key] = definition
        ini_cont.write(','.join(definitions.keys()) + '\r\n\r\n')
        for key, defini in definitions.items():
            ini_cont.write('[%s_%s]\r\n' % (topic[:-1], key))
            for k, v in defini.items():
                ini_cont.write('%s=%s\r\n' % (k, v))
            ini_cont.write('\r\n')
    # 根据配置创建loggers
    #print ini_cont.getvalue()
    ini_cont.seek(0)
    return ini_cont