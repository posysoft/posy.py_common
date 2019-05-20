# coding:utf-8
__author__ = 'HuangZhi'

import sys
import os
import logging as log

from utilities import config_wrapper as CfgWrapper
from utilities.little_tools import import_object


class Framework(object):
    _inst = None

    @staticmethod
    def getInst():
        if Framework._inst is None:
        #    Framework._inst = Framework()
            raise Exception('Framework not initialized')
        return Framework._inst

    def __init__(self):
        super(Framework, self).__init__()
        if Framework._inst is not None:
            raise Exception('Framework can be initialized just once')
        Framework._inst = self
        self._globals = {}

    def __getattr__(self,  item):
        if item in self._globals:
            return self._globals[item]
        else:
            raise AttributeError(item)

    @staticmethod
    def registerGlobal(name, obj):
        Framework.getInst()._globals[name] = obj

    @staticmethod
    def retrieveGlobal(name, default=None):
        return Framework.getInst()._globals.get(name, default)

    @staticmethod
    def hasGlobal(name):
        return name in Framework.getInst()._globals

    @staticmethod
    def unregisterGlobal(name):
        del Framework.getInst()._globals[name]

    @staticmethod
    def loadSetupConfig(setup_file, cfg):
        if os.path.exists(setup_file):
            setup = CfgWrapper.loadJsonConfig(setup_file, expand_externals=False)
            CfgWrapper.mergeConfig(setup, cfg)
            CfgWrapper.expandConfigExternals(setup)
            return setup
        else:
            return cfg

    #acceptable named args:
    #   app_type, cfg_file, hbase, redis, mysql, event_queue, scheduled_task, async_tasks,
    #   http_server, static_links, hbase_links, rpc_server
    def initApplication(self, **app_info):
        app_info = dict(app_info)
        Framework.registerGlobal('app_info', app_info)

        exec_name = os.path.basename(sys.argv[0]).split('.')
        exec_name = '.'.join(exec_name[: -1 if len(exec_name) > 1 else None])
        app_info['app_type'] = app_info.get('app_type', exec_name)

        #load configuration file
        app_info['cfg_file'] = app_info.get('cfg_file', sys.argv[1] if len(sys.argv) >= 2 else
                                            ("../config/local/"+app_info['app_type']+".default.json"))
        cfg = CfgWrapper.loadJsonConfig(app_info['cfg_file'])
        cfg = Framework.loadSetupConfig(app_info.get('setup', './'+app_info['app_type']+".setup.json"), cfg)
        cfg_root = CfgWrapper.CfgWrapper(cfg)
        Framework.registerGlobal("cfg", cfg)
        Framework.registerGlobal("cfg_root", cfg_root)

        app_info['app_name'] = app_info.get('app_name', cfg_root.getConfig("application/name"))

        # loading logging
        cfg_log = cfg_root.getConfig('application/log')
        if cfg_log:
            from utilities.log_wrapper import load_dict_config
            load_dict_config(cfg_root.getConfig('application/log'), app_info)
            Framework.registerGlobal("log", log)

        # prepare hbase wrapper
        cfg_hbase = cfg_root.getConfig('application/hbase', app_info.get('hbase', None))
        if cfg_hbase is not None:
            from utilities.hbase_wrapper import HbaseConnPool
            hbase = HbaseConnPool(**cfg_hbase['server'])
            hbase.set_log(self.log)
            Framework.registerGlobal("hbase", hbase)

        # prepare redis wrapper
        cfg_redis = cfg_root.getConfig('application/redis', app_info.get('redis', None))
        if cfg_redis:
            from utilities.redis_wrapper import RedisConnPool
            redis = RedisConnPool(**cfg_redis['server'])
            Framework.registerGlobal("redis", redis)

        # prepare mysql wrapper
        cfg_mysql = cfg_root.getConfig('application/mysql', app_info.get('mysql', None))
        if cfg_mysql:
            from utilities.db_wrapper import DBConnPool
            servers = {}
            if 'server' in cfg_mysql:
                mysql = DBConnPool(**cfg_mysql['server'])
                servers['default'] = mysql
            if 'servers' in cfg_mysql:
                for n,s in cfg_mysql['servers'].items():
                    mysql = DBConnPool(**s)
                    servers[n] = mysql

            if 'default' not in servers and 'master' in servers:
                servers['default'] = servers['master']

            for n, s in servers.items():
                if n == 'default':
                    Framework.registerGlobal("mysql", s)
                else:
                    Framework.registerGlobal("mysql."+n, s)

        # prepare event-queue
        cfg_eq = cfg_root.getConfig('application/event_queue', app_info.get('event_queue', None))
        if cfg_eq:
            from event_queue import EventQueue
            eq = EventQueue()
            Framework.registerGlobal("event_queue", eq)

        # prepare scheduled tasks
        cfg_st = cfg_root.getConfig('application/scheduled_task', app_info.get('scheduled_task', None))
        if cfg_st:
            from utilities.scheduled_task import TaskScheduler
            scheduled_tasks = TaskScheduler()
            Framework.registerGlobal('scheduled_tasks', scheduled_tasks)

        # prepare asynchronous tasks
        cfg_at = cfg_root.getConfig('application/async_tasks', app_info.get('async_tasks', None))
        if cfg_at:
            from async_task.async_task import AsyncTaskManager
            async_tasks = AsyncTaskManager(cfg_root.getConfig('application/async_tasks.default_capacity', 1))
            async_tasks.setLog(log)
            Framework.registerGlobal('async_tasks', async_tasks)

        # init alarm email
        cfg_alarm = cfg_root.getConfig('application/alarm_email')
        if cfg_alarm:
            from utilities.smtp import Email
            alarm_email = Email(cfg_alarm, self.log)
            Framework.registerGlobal('alarm_email', alarm_email)

        # prepare http service framework
        cfg_http = cfg_root.getConfig('application/http_server', app_info.get('http_server', None))
        if cfg_http:
            from http_service.http_server import HttpServer
            http_server = HttpServer(**cfg_http['listener'])
            Framework.registerGlobal('http_server', http_server)

        # prepare http middleware
        cfg_middleware = cfg_root.getConfig('application/http_server/middleware_class')

        # prepare event
        cfg_event = cfg_root.getConfig('application/event')

        # init custom resources
        self.onAppInit()

        # load schedule tasks
        if cfg_st:
            self.onLoadScheduledTask(self.scheduled_tasks, cfg_st)

        # load async tasks
        if cfg_at:
            self.onLoadAsyncTasks(self.async_tasks, cfg_at)

        # load http handlers
        if cfg_http:
            self.onLoadHttpHandlers(self.http_server, cfg_http)

        # load http middleware
        if cfg_middleware:
            self.onLoadHttpMiddleware(cfg_middleware)

        # load event observers and handlers
        if cfg_event:
            self.onLoadEvents(cfg_event)

        #init custom resources
        self.onAppInited()

    #to be override by subclass, to load scheduled tasks
    def onLoadScheduledTasks(self, scheduled_tasks, cfg):
        #from scheduled_tasks import *
        return

    #to be override by subclass, to load asynchronous task pool
    def onLoadAsyncTasks(self, async_tasks, cfg):
        #from processors import *
        for p in cfg:
            if p['proc_type'].startswith('class:'):
                proc_class = import_object(p['proc_type'].split(':', 1)[1])
                async_tasks.registerProcessor(p['proc_type'], proc_class)
            async_tasks.createWorkerPool(**p)
        return

    # load http middleware
    def onLoadHttpMiddleware(self, mw_cfg):
        for cls_path in mw_cfg:
            cls = import_object(cls_path)
            self.http_server.register_middleware(cls())
            log.info('register http middleware %s' % cls.__name__)

    def onLoadHttpHandlers(self, http_server, cfg):
        cfg_http_static = cfg.get('static_links', self.app_info.get('static_links', None))
        if cfg_http_static is not None:
            from http_service.static_handler import StaticHandler
            StaticHandler.batchRegister(cfg_http_static, http_server, log)

        cfg_http_hbase = cfg.get('hbase_links', self.app_info.get('hbase_links', None))
        if cfg_http_hbase is not None:
            from http_service.hbase_handler import DomainHbaseHandler
            DomainHbaseHandler.batchRegister(cfg_http_hbase, Framework.retrieveGlobal('hbase'), http_server, log)

        cfg_http_sys_stat = cfg.get('sys_stat', self.app_info.get('sys_stat', None))
        if cfg_http_sys_stat is not None:
            from http_service.sys_stat_handler import SysStatHandler
            SysStatHandler(*cfg_http_sys_stat).setLog(log).register(http_server)

        cfg_http_rpc = cfg.get('rpc_server', self.app_info.get('rpc_server', None))
        if cfg_http_rpc is not None:
            from http_service.json_cmd_handler import HttpCmdDispatcher
            rpc_server = HttpCmdDispatcher()
            rpc_server.register(cfg_http_rpc['path']+'/*', http_server)
            self.log.info("register rpc interface: path=%s/*" % cfg_http_rpc['path'])
            if 'templates' in cfg_http_rpc:
                path = cfg_http_rpc['templates'][0]
                local = cfg_http_rpc['templates'][1]
                rpc_server.register(path+'/*', http_server)
                rpc_server.setTemplates(path, local)
                self.log.info("register dynamic pages: path=%s/*, local=%s" % (path, local))
            if 'authorization' in cfg_http_rpc:
                rpc_server.setAuthorizeInfo(cfg_http_rpc['authorization'])
            Framework.registerGlobal('rpc_server', rpc_server)
            self.onLoadRpcCommands(rpc_server, cfg_http_rpc)
        return

    # load event manager
    def onLoadEvents(self, evt_cfg):
        em = Framework.retrieveGlobal('event_manager')
        parents = [em]
        layer_parsers = [evt_cfg.get('parsers')]
        while layer_parsers:
            _lay_prs = layer_parsers
            _parents = parents
            parents = []
            layer_parsers = []
            i = 0
            for node_parsers in _lay_prs:
                parent = _parents[i]
                i += 1
                if not node_parsers:
                    continue
                for conf in node_parsers:
                    parser_path = conf['class']
                    cls = import_object(parser_path)
                    parser = parent.register_parser(cls, conf.get('cfg'))
                    parents.append(parser)
                    layer_parsers.append(conf.get('sub_parsers', []))
                    log.info('register event parser %s to %s' % (cls.__name__, parent.__class__.__name__))

        # load event observers
        import types
        classobj = type(object)
        for obv_cfg in evt_cfg.get('observers', []):
            obv_cls = import_object(obv_cfg['class'])
            hdl = import_object(obv_cfg['handler'])
            if isinstance(hdl, (types.ClassType, classobj)):
                hdl = hdl()
            hdl = getattr(hdl, 'handle', None) or hdl
            obv = obv_cls(hdl, obv_cfg['priority'])
            em.monitor.register_observer(obv_cfg['event'], obv)
            log.info('register observer %s to event monitor' % obv_cfg['event'])

    def onLoadRpcCommands(self, rpc_server, cfg):
        #from operations import *
        from http_service.json_cmd_handler import CmdAsyncTaskHandler

        commands = cfg.get('commands')
        if commands is not None:
            for c in commands:
                if isinstance(c, dict) and c['handler'].startswith('async_tasks:'):
                    CmdAsyncTaskHandler(self.async_tasks, c['handler'].split(':', 1)[1]).\
                        register(c['handler'], rpc_server).setLog(self.log)
                elif isinstance(c, dict) and c['handler'].startswith('class:'):
                    proc_class = import_object(c['handler'].split(':', 1)[1])
                    proc_class().register(c['handler'], rpc_server).setLog(self.log)


        rpc_server.loadCommands(commands)
        return

    def onAppInited(self):
        pass

    def onAppInit(self):
        pass
