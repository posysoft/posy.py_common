# coding:utf-8
__author__ = '. at 13-11-11'

import time
import gevent
import traceback


class TimeDriver(object):
    """
    时间触发器
    """
    def __init__(self):
        self.delay_calls = {}

    def call_later(self, s, f, *args, **kwargs):
        """
        s : delay seconds
        f : function or method
        args : the args of f
        kwargs : the kwargs of f
        """
        ct = int(time.time()) + s
        d = DelayCall(ct, f, *args, **kwargs)
        self.add_call(ct, d)
        return d

    def looping_call(self, s, itv, f, *args, **kwargs):
        ct = int(time.time()) + s
        c = LoopingCall(ct, itv, f, *args, **kwargs)
        self.add_call(ct, c)
        return c

    def add_call(self, ct, call):
        if ct in self.delay_calls:
            self.delay_calls[ct].append(call)
        else:
            self.delay_calls[ct] = [call]
        return call

    def remove_call(self, ct, dc):
        """
        ct : call time
        dc : delay call
        """
        if ct in self.delay_calls:
            if dc in self.delay_calls[ct]:
                self.delay_calls[ct].remove(dc)

    def start(self):
        return gevent.spawn(self.drive)

    def drive(self):
        while True:
            self.process()
            gevent.sleep(1)

    def process(self):
        call_times = self.delay_calls.keys()
        call_times.sort()
        now_time = int(time.time())
        for call_time in call_times:
            if call_time > now_time:
                break
            ds = self.delay_calls.pop(call_time)
            for d in ds:
                d.call()


time_driver = TimeDriver()
__g = time_driver.start()


class DelayCall(object):
    """
    延迟回调对象
    """
    def __init__(self, ct, func, *args, **kwargs):
        self.driver = time_driver
        self.call_time = ct
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.result = None
        self.exc = None
        self.called = False
        self.canceled = False

    def call(self):
        try:
            self.result = self.func(*self.args, **self.kwargs)
            self.called = True
        except Exception, e:
            self.exc = e
            print traceback.print_exc()

    def cancel(self):
        self.driver.remove_call(self.call_time, self)
        self.canceled = True


class LoopingCall(DelayCall):
    """
    循环调用
    """
    def __init__(self, ct, interval, func, *args, **kwargs):
        super(LoopingCall, self).__init__(ct, func, *args, **kwargs)
        self.interval = interval

    def call(self):
        try:
            self.result = self.func(*self.args, **self.kwargs)
        except Exception, e:
            self.exc = e
        finally:
            self.call_time += self.interval
            self.driver.add_call(self.call_time, self)


__all__ = ['time_driver']


def test():
    def c1():
        print 'c1', int(time.time())

    def c2():
        print 'c2', int(time.time())

    def c3():
        while 1:
            print 'c3', int(time.time())
            gevent.sleep(3)

    print int(time.time())
    time_driver.call_later(2, c1)
    time_driver.looping_call(2, 3, c2)
    time_driver.call_later(2, gevent.spawn, c3)


if __name__ == '__main__':
    test()
    __g.join()