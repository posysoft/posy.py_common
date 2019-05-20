# coding:utf-8
# Created by lilx on 15/6/14.
__author__ = 'lilx'

from event_monitor import EventMonitor
from event_monitor import NullObject


class EventManager(object):
    """
    event manager
    """
    def __init__(self, event_queue=None, log=NullObject(), auto_notify=True):
        self.monitor = EventMonitor(event_queue, log, auto_notify)
        self.log = log
        self.parsers = []

    def register_parser(self, parser_class, cfg=None):
        parser = parser_class(self.monitor, self.log, cfg=cfg)
        self.parsers.append(parser)
        return parser

    def event_parse(self, source):
        for p in self.parsers:
            if p.parse(source):
                return True


class EventParser(object):
    """
    event generator
    """
    NAME = 'EventParser'

    def __init__(self, monitor, log=NullObject(), parent=None, cfg=None):
        self.monitor = monitor
        self.log = log
        self.parent = parent
        self.cfg = cfg
        self.sub_parsers = []

    def register_parser(self, parser_class, cfg=None):
        parser = parser_class(self.monitor, self.log, self, cfg)
        self.sub_parsers.append(parser)
        return parser

    def filter(self, source):
        pass

    def generate(self, source):
        pass

    def parse(self, source):
        self.log.debug('parsing event with %s (%s)' % (self.__class__.__name__, id(source)))
        if not self.filter(source):
            return False
        e = self.generate(source)
        if e:
            self.monitor.send_event(e)
            self.log.debug('event was found with %s (%s)' % (self.__class__.__name__, id(source)))
            return True
        for sp in self.sub_parsers:
            if sp.parse(source):
                return True
        return False