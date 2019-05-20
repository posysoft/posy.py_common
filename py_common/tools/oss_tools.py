# coding:utf-8
__author__ = 'HuangZhi'

OSS_ID = '8ipOyISG6UlX9hUT'
OSS_KEY = 'jsidwXZNPNxs1nvwIRMMUdOeeAlCpJ'
OSS_BUCKET = 'jiaowang'
OSS_SERVER = 'oss-cn-shanghai.aliyuncs.com'


import os
import glob
import sys
from oss.oss_api import *
from xml.etree import ElementTree


def showHelp(*params, **flags):
    print 'USAGE: %s [flags] command [params]' % (os.path.basename(sys.argv[0]), )
    print 'COMMANDS:'
    print '  help: show this help screen'
    print '  upload file_name_patterns: upload files onto the oss server'
    print '    -prefix=oss_prefix: prefix of object name in the oss bucket'
    print 'COMMON FLAGS:'
    print '  -server=oss_server: specify the oss server, current is %s' % (flags['server'],)
    print '  -id=oss_secret_id: secret id for the server'
    print '  -key=oss_secret_key: secret key for the server'
    print '  -bucket=oss_bucket: specify the oss bucket, current is %s' % (flags['bucket'],)
    print '  -silent: run in silent mode'


def uploadFiles(*params, **flags):
    if __name__ != '__main__':
        flags['silent'] = ''
    oss = OssAPI(flags['server'], flags['id'], flags['key'])

    count = 0
    fails = []

    for patterns in params:
        patterns = patterns.split(',')
        for pattern in patterns:
            for f in glob.glob(pattern):
                count += 1
                n = os.path.basename(f)
                t = flags.get('prefix', '') + n
#                print "%d: %s -> %s" % (count, n, t)
                res = oss.put_object_from_file(flags['bucket'], t, f)
                if 'silent' not in flags:
                    print "%d: %s -> %s : %d" % (count, n, t, res.status)
                if res.status != 200:
                    fails.append(n)
                    print res.read()

    if 'silent' not in flags:
        print 'Upload %d files with %d fails' % (count, len(fails))
        for f in fails:
            print '  Fail: %s' % (f, )
    return count, fails


if __name__ == '__main__':
    cmd = None
    flags = {'id': OSS_ID, 'key': OSS_KEY, 'bucket': OSS_BUCKET, 'server': OSS_SERVER}
    params = []

    for i in range(1, len(sys.argv)):
        a = sys.argv[i]
        if (len(a) > 1) and (a[0] == '-'):  # flags
            n = a[1:].split('=')[0]
            v = a[len(n)+2:]
            flags[n] = v
        elif not cmd:   # command
            cmd = a
        else:   # parameters
            params.append(a)

    #print flags, cmd, params
    if (not cmd) or (cmd == 'help'):
        showHelp(*params, **flags)
    elif cmd == 'upload':
        uploadFiles(*params, **flags)
    else:
        print 'Unkown command - %s' % cmd




