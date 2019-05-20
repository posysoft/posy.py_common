# coding:utf-8
__author__ = 'HuangZhi'

from collections import deque


class EventQueue():
    def __init__(self):
        self.event_map = {}
        self.event_queue = deque()

    def __del__(self):
        pass

    def registerHandler(self, type, handler, filter=None, creator=None):
        handlers = self.event_map[type] if (type in self.event_map) else {}
        handlers[handler] = (filter, creator)
        self.event_map[type] = handlers

    def unregisterHandler(self, type, handler):
        if type in self.event_map:
            handlers = self.event_map[type]
            del handlers[handler]

    def sendEvent(self, type, param):
        self.event_queue.append((type, param))

    def purgeEvents(self):
        while len(self.event_queue) > 0:
            self._processEvent(self.event_queue.popleft())

    def _processEvent(self, event):
        if event[0] in self.event_map:
            handlers = self.event_map[event[0]]
            for handler, (filter, creator) in handlers.items():
                if (not filter) or filter(event):
                    if creator:
                        handler(creator(), event[0], event[1])
                    else:
                        handler(event[0], event[1])
        else:
            pass  # print 'unhandled event:', event[0]
