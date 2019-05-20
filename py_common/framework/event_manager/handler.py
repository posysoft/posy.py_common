# coding:utf-8
# Created by lilx on 15/6/10.
__author__ = 'lilx'


class EventHandler(object):
    """
    basic event handler
    """
    EVENT = None

    def __call__(self, *args, **kwargs):
        return self.handler(*args, **kwargs)

    def handler(self, event):
        pass