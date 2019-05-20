# coding:utf-8
__author__ = 'HuangZhi'


import time
import traceback
from framework.utilities.time_driver import time_driver


class TaskScheduler():
    def __init__(self):
        self.tasks = {}
        self.timer = time_driver
        self.processors = {}

    def registerTaskProcessor(self, task_type, processor):
        self.processors[task_type] = processor

    def scheduleTask(self, id, schedule_info, task_info):
        now = long(time.time())
        start = now
        if schedule_info[0]:
            start = str(schedule_info[0])
            start = time.strptime(start, '%Y%m%d%H%M')
            start = long(time.mktime(start))
        interval = long(schedule_info[1] * 60) if len(schedule_info) > 1 else 1
        repeat = long(schedule_info[2] if len(schedule_info) > 2 else 100000000) if len(schedule_info) > 1 else 1
        repeat_index = (now - start + interval - 1) / interval
        repeat_index = min(max(repeat_index, 0), repeat)
        next_time = start + interval * repeat_index

        task = {
            'id': id,
            'start': start,
            'interval': interval,
            'repeat': repeat,
            'repeat_index': repeat_index,
            'next_time': next_time,
            'task_info': task_info,
            'timer_stub': None
        }
        self.tasks[id] = task

        if repeat_index < repeat:
            task['timer_stub'] = self.timer.call_later(next_time - now, self._launchTask, task)

    def cancelTask(self, id):
        if id in self.tasks:
            task = self.tasks[id]
            if task['timer_stub']:
                task['timer_stub'].cancel()
            del self.tasks[id]

    def _launchTask(self, task):
        try:
            info = task['task_info']
            processor = self.processors[info[0]]
            processor(info, task['repeat_index'])
        except:
            print traceback.print_exc()

        task['repeat_index'] += 1
        task['next_time'] = task['start'] + task['interval'] * task['repeat_index']
        if task['repeat_index'] < task['repeat']:
            task['timer_stub'] = self.timer.call_later(task['next_time'] - time.time(), self._launchTask, task)


