# coding:utf-8
__author__ = 'HuangZhi'

from gevent.subprocess import *
import shlex

from framework.async_task.async_task import BasicAsyncProcessor


class SubprocessProcessor(BasicAsyncProcessor):
    def __init__(self, cmd, log=None):
        super(SubprocessProcessor, self).__init__(log)
        self.cmd = cmd
        self.popen = None

    def onTask(self, req, priority):
        """
        req = {
            'input_folder': self.docs_folder,  # tmp/local
            'output_folder': index_folder,     # tmp/1444.index
            'output_package': index_package    # tmp/1444.tgz
        }
        :param req:
        :param priority:
        :return:
        """
        if not self.isSubprocessAlive():
            self.openSubprocess(req, priority)  # execute a command
            self.onSubprocessOpened()
        res = self.doProcessTask(self.popen.stdin, self.popen.stdout, req, priority)
        return res

    def doProcessTask(self, pipe_in, pipe_out, req, priority):
        pass

    def onSubprocessOpened(self):
        pass

    def getSubprocessCmd(self, req, priority):
        return self.cmd.encode('ascii')

    def openSubprocess(self, req, priority):
        cmd = self.getSubprocessCmd(req, priority)
        args = shlex.split(cmd)
        if self.log:
            self.log.debug('Open subprocess index=%d, cmd="%s"' % (self.index, cmd))
        self.popen = Popen(args, stdin=PIPE, stdout=PIPE, shell=False)

    def writeSubprocess(self, data):
        self.popen.stdin.write(data)

    def readSubprocess(self, length='line'):
        if length == 'line':
            self.popen.stdout.readline()

    def isSubprocessAlive(self):
        if self.popen is None:
            return False
        r = self.popen.poll()
        return r is None

