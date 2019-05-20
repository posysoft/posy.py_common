# coding:utf-8
__author__ = 'HuangZhi'

import json#ujson as json
import types
import os


def loadJsonConfig(file_name, base_path=None, expand_externals=True):
    path = file_name if (base_path is None) else base_path + '/' + file_name
    with open(path, 'r') as f:
        result = json.load(f)
    if expand_externals:
        result = expandConfigExternals(result, os.path.dirname(path))
    return result


def getConfig(cfg, path, default=None):
    try:
        ns = path.split('/')
        for n in ns:
            if n:
                if isinstance(cfg, list):
                    cfg = cfg[int(n)]
                elif isinstance(cfg, dict):
                    cfg = cfg[n]
        return cfg
    except Exception:
        return default


def mergeConfig(default, new):
    if isinstance(default, list) and isinstance(new, list):
        default.extend(new)
    elif isinstance(default, dict) and isinstance(new, dict):
        for (k, v) in new.items():
            if k in default:
                default[k] = mergeConfig(default[k], v)
            else:
                default[k] = v
    else:
        default = new
    return default


EXTERNAL_PREFIX = '<<external:'
EXTERNAL_POSTFIX = '>>'


def expandConfigExternals(cfg, base_path=None, root=None):
    if root is None:
        root = cfg
    if isinstance(cfg, list):
        for i in range(len(cfg)):
            cfg[i] = expandConfigExternals(cfg[i], base_path, root)
    elif isinstance(cfg, dict):
        for (k, v) in cfg.items():
            cfg[k] = expandConfigExternals(v, base_path, root)
    elif isinstance(cfg, unicode):
        if len(cfg) > len(EXTERNAL_PREFIX) + len(EXTERNAL_POSTFIX):
            if (cfg[:len(EXTERNAL_PREFIX)] == EXTERNAL_PREFIX) and (cfg[-len(EXTERNAL_POSTFIX):] == EXTERNAL_POSTFIX):
                fs = cfg[len(EXTERNAL_PREFIX):-len(EXTERNAL_POSTFIX)].split(',')
                cfg = None
                for f in fs:
                    fp = f.split('#')
                    fn = fp[0].strip()
                    fp = fp[1] if len(fp) > 1 else ''
                    if len(fn) > 0:
                        c = loadJsonConfig(fn, base_path, True)
                    else:
                        c = root
                    c = getConfig(c, fp)
                    if isinstance(cfg, list):
                        cfg.extend(c)
                    elif isinstance(cfg, dict):
                        cfg.update(c)
                    else:
                        cfg = c
    return cfg


class CfgWrapper():
    def __init__(self, cfg):
        self.cfg = cfg

    def getConfig(self, path, default=None):
        return getConfig(self.cfg, path, default)

