# coding:utf-8
# Created by lilx on 15/6/8.
__author__ = 'lilx'


class Observer(object):
    """
    Observer base class
    """
    NAME = None

    def __init__(self, handle=None, priority=10):
        if handle:
            self.handle = handle
        self.priority = priority

    def handle(self, event):
        pass

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)

    def notify_observer(self, event):
        self.handle(event)

    def register(self, monitor, event_name=None):
        monitor.register_observer(event_name or self.NAME, self)


class Event(object):
    """
    event entity
    """
    def __init__(self, name, body, priority=10):
        self.name = name
        self.body = body
        self.priority = priority

    def __cmp__(self, other):
        return cmp(self.priority, other.priority)