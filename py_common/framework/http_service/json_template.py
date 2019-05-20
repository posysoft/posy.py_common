# coding:utf-8
__author__ = 'HuangZhi'

import json
import copy
from framework.utilities.little_tools import FileExtIdentifier
from framework.utilities.config_wrapper import CfgWrapper


class JsonTemplates(object):
    def __init__(self, path):
        super(JsonTemplates, self).__init__()
        self.base_path = path
        self.templates = {}

    def fetchTemplate(self, file_name):
        template = self.templates.get(file_name)
        if template is None:
            template = JsonTemplate(self.base_path + '/' + file_name)
            #self.templates[file_name] = template
        return template


class JsonTemplate(object):
    TAG_PREFIX = "'<<"
    TAG_POSTFIX = ">>'"

    def __init__(self, file_name):
        super(JsonTemplate, self).__init__()
        self._loadTemplate(file_name)

    def _loadTemplate(self, file_name):
        with open(file_name, 'rb') as f:
            self.content = f.read()

        self.content_type = FileExtIdentifier.identifyExtFormat(file_name)
        self.operation = None
        self.processor = None
        self.flags = {}

        self.tags = []
        pos = 0
        while True:
            tag_start = self.content.find(self.TAG_PREFIX, pos)
            if tag_start == -1:
                break
            tag_stop = self.content.find(self.TAG_POSTFIX, tag_start+len(self.TAG_PREFIX))
            tag_string = self.content[tag_start+len(self.TAG_PREFIX): tag_stop]
            tag_stop += len(self.TAG_POSTFIX)
            pos = tag_stop

            tag = {'start': tag_start, 'stop': tag_stop}
            for t in tag_string.split(';'):
                if not t.strip(' \t\r\n'):
                    continue

                inst = t[: t.find('=')]
                param = t[len(inst)+1:].strip(' \t\r\n').strip('\'\"')
                inst = inst.strip(' \t\r\n')

                if inst == 'content_type':
                    self.content_type = param
                elif inst == 'operation':
                    self.operation = param
                elif inst == 'processor':
                    self.processor = param
                elif inst == 'flags':
                    self.flags = json.loads(param)
                elif inst == 'insert':
                    tag['insert'] = param
                elif not param:
                    tag['insert'] = inst

            self.tags.append(tag)

    def getOperation(self):
        return self.operation

    def getProcessor(self):
        return self.processor

    def getFlags(self):
        return self.flags

    def getContentType(self):
        return self.content_type if self.content_type else 'text/plain'

    def join(self, values):
        values = CfgWrapper(values)

        content = ''
        pos = 0
        for tag in self.tags:
            content += self.content[pos: tag['start']]
            if 'insert' in tag:
                content += json.dumps(values.getConfig(tag['insert']))
            pos = tag['stop']
        content += self.content[pos:]

        return content





