# coding:utf-8
__author__ = 'HuangZhi'

from gevent.queue import PriorityQueue as Queue
from gevent.queue import Empty as EmptyException
from gevent.event import Event
import gevent
import time
import traceback


class AsyncTaskManager(object):
    def __init__(self, default_capacity=1):
        super(AsyncTaskManager, self).__init__()
        self.procs = {}
        self.pools = {}
        self.log = None
        self.default_capacity = default_capacity

    def registerProcessor(self, proc_type, proc_creator):
        proc = {
            'type': proc_type,
            'creator': proc_creator
        }
        self.procs[proc_type] = proc
        if self.log:
            self.log.info("register async-processor: %s" % proc_type)

    def createWorkerPool(self, pool_name, proc_type, capacity=None, proc_params=None):
        capacity = capacity or self.default_capacity
        if self.log:
            self.log.info("create async-task pool: %s, processor=%s, capacity=%d" % (pool_name, proc_type, capacity))
        proc = self.procs[proc_type]
        pool = {
            'type': proc_type,
            'processor': (proc['creator'], proc_params if proc_params is not None else {}),
            'capacity': 0,
            'next_index': 0L,
            'queue': Queue(),
            'working_count': 0,
            'time_summary': 0.,
            'task_summary': 0L,
            'summary_start': time.time()
        }
        self.pools[pool_name] = pool
        self.setWorkerPoolCapacity(pool_name, capacity)

    def getPoolStatistics(self, pool_name, reset=False):
        pool = self.pools[pool_name]
        t = time.time()
        res = {
            'name': pool_name,
            'type': pool['type'],
            'capacity': pool['capacity'],
            'in_use': pool['working_count'],
            'workload': pool['time_summary'] / (t - pool['summary_start']) / pool['capacity'],
            'queued_task': pool['queue'].qsize(),
            'avg_time': (pool['time_summary'] / pool['task_summary']) if pool['task_summary'] else None,
            'task_count': pool['task_summary'],
            'task_flow': pool['task_summary'] / (t - pool['summary_start'])
        }

        if reset:
            pool['time_summary'] = 0.
            pool['task_summary'] = 0L
            pool['summary_start'] = t
        return res

    def getPoolList(self):
        return self.pools.keys()

    def setWorkerPoolCapacity(self, pool_name, capacity):
        pool = self.pools[pool_name]
        old_cap = pool['capacity']
        pool['capacity'] = capacity
        for index in range(old_cap, capacity):
            gevent.spawn(self._procTask, pool, index)

    def getWorkerPoolCapacity(self, pool_name):
        return self.pools[pool_name]['capacity']

    def postTasks(self, reqs, stub=None):
        if hasattr(reqs, 'keys'):
            for k in reqs.keys():
                req = reqs[k]
                stub = self.postTask(req[0], req[1], req[2] if len(req) > 2 else 0, k, stub)
        else:
            for req in reqs:
                stub = self.postTask(req[0], req[1], req[2] if len(req) > 2 else 0, None, stub)
        return stub

    def postTask(self, pool_name, req, priority=0, key=None, stub=None):
        stub = stub if stub else AsyncTaskStub()
        task = stub._addRequest(key, pool_name, req, priority)
        pool = self.pools[pool_name]
        pool['queue'].put((priority, pool['next_index'], task))
        pool['next_index'] += 1
        return stub

    def _procTask(self, pool, index):
        proc_creator, proc_params = pool['processor']
        proc_params = dict(proc_params)
        processor = proc_creator(**proc_params)
        if hasattr(processor, 'onEnter'):
            processor.onEnter(index)
        while index < pool['capacity']:
            try:
                priority, _index, task = pool['queue'].get(timeout=1)
            except EmptyException:
                continue

            start_time = time.time()
            pool['working_count'] += 1
            if self.log:
                self.log.debug('Process task %s by processor %d, priority:%d' % (pool['type'], index, priority))

            res = None
            try:
                res = processor.onTask(task['req'], priority)
            except BaseException, e:
                if self.log:
                    self.log.error('Process async-task failed: %s' % e)
                    self.log.error(traceback.format_exc())
            AsyncTaskStub._finishTask(task, res)

            pool['working_count'] -= 1
            pool['task_summary'] += 1
            pool['time_summary'] += time.time() - start_time

        if hasattr(processor, 'onExit'):
            processor.onExit()

    def setLog(self, log):
        self.log = log


class AsyncTaskStub(object):
    def __init__(self):
        super(AsyncTaskStub, self).__init__()
        self.tasks = {}
        self._next_key = 0
        self.event = Event()

    def getTaskCount(self):
        return len(self.tasks)

    def waitForResult(self, wait_for_all=True, timeout=None):
        end_time = time.time() + timeout if timeout is not None else None
        while True:
            finished = self.getFinishedKeys()
            if wait_for_all:
                if len(finished) == len(self.tasks):
                    return finished
            else:
                if len(finished) > 0:
                    return finished
            if end_time:
                timeout = end_time - time.time()
                if timeout <= 0.0:
                    return finished
            self.event.clear()
            self.event.wait(timeout)

    def isFinished(self, key=None):
        key = key if key is not None else self.tasks.keys()[0]
        return self.tasks[key]['finished']

    def getResult(self, key=None, auto_abandon=True):
        key = key if key is not None else self.tasks.keys()[0]
        task = self.tasks[key]
        if task['finished'] and auto_abandon:
            del self.tasks[key]
        return task['res']

    def getFinishedKeys(self):
        keys = []
        for task in self.tasks.itervalues():
            if task['finished']:
                keys.append(task['key'])
        return keys

    def abandonTask(self, key):
        del self.tasks[key]

    def _addRequest(self, key, pool_name, req, priority):
        task = {
            'key': key if (key is not None) else self._next_key,
            'pool': pool_name,
            'priority': priority,
            'req': req,
            'res': None,
            'finished': False,
            'event': self.event
        }
        self._next_key += 1
        self.tasks[task['key']] = task
        return task

    @staticmethod
    def _finishTask(task, res):
        task['res'] = res
        task['finished'] = True
        task['event'].set()


# leaky bucket of concurrent async-tasks
# on_finish(key, res)

class AsyncTaskBucket(AsyncTaskStub):
    def __init__(self, on_finish=None):
        super(AsyncTaskBucket, self).__init__()
        self.on_finish = on_finish

    def restrict(self, capacity, timeout=None):
        while self.getTaskCount() > capacity:
            ks = self.waitForResult(wait_for_all=False, timeout=timeout)
            for k in ks:
                res = self.getResult(k)
                self.onTaskFinish(k, res)

    def flush(self, timeout=None):
        self.restrict(0, timeout)

    def onTaskFinish(self, key, res):
        if self.on_finish:
            self.on_finish(key, res)


class BasicAsyncProcessor(object):
    def __init__(self, log=None):
        self.log = log

    def setLog(self, log):
        self.log = log

    def onEnter(self, index):
        self.index = index

    def onTask(self, req, priority):
        pass

    def onExit(self):
        pass


class BasicCmdAsyncProcessor(BasicAsyncProcessor):
    def __init__(self, log=None):
        super(BasicCmdAsyncProcessor, self).__init__(log)

    def onRequest(self, request):
        """
        :param request: req['content'] that is the req data
        :return:
        """
        return 501, {'result': 'NOT_IMPLEMENTED'}

    def onTask(self, req, priority):
        return self.onRequest(req['content'])


class SimpleCmdAsyncHandler(BasicCmdAsyncProcessor):
    """
    simple rpc command handler with req object
    """
    def __init__(self, log=None):
        super(SimpleCmdAsyncHandler, self).__init__(log)

    def onRequest(self, request):
        try:
            return self.doRequest(request['operation'], request['request'])
        except BaseException, e:
            self.log.error('Operation [' + request['operation'] + '] processing failed')
            self.log.error(traceback.format_exc())
            return {'result': 'BAD_REQUEST'}

    def doRequest(self, ope, req):
        """
        :param ope: operation name
        :param req: request params
        :return:
        """
        return 501, {'result': 'NOT_IMPLEMENTED'}

