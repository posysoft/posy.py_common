# coding:utf-8
# Created by lilx on 15/6/8.
__author__ = 'lilx'

from gevent.queue import Queue
import bisect
import gevent


class EventMonitor(object):
    """
    event monitor
    """
    def __init__(self, event_queue=None, log=None, auto_notify=True):
        if not event_queue:
            self.events = Queue()
        else:
            self.events = event_queue
        if not log:
            self.log = NullObject()
        else:
            self.log = log
        self.observers = {}
        self.__notifying = False
        if auto_notify:
            self.__g = gevent.spawn(self.notify_observers)

    def register_observer(self, event_name, observer):
        oq = self.observers.get(event_name, [])
        bisect.insort(oq, observer)
        self.observers[event_name] = oq

    def notify_observer(self, event):
        oq = self.observers.get(event.name, [])
        for ob in oq:
            try:
                ob.notify_observer(event)
                self.log.debug('notify observer %s with event %s' % (id(ob), id(event)))
            except Exception, e:
                self.log.exception(repr(e))

    def send_event(self, event):
        self.events.put(event)
        self.log.debug('receive event %s' % id(event))

    def notify_observers(self):
        self.__notifying = True
        while True:
            evt = self.events.get()
            self.notify_observer(evt)
        self.__notifying = False

    def do_notify(self):
        if not self.__notifying:
            self.notify_observers()


class NullObject(object):
    """
    do nothing object
    """
    def __getattr__(self, item):
        return self

    def __str__(self):
        return ''

    def __call__(self, *args, **kwargs):
        pass