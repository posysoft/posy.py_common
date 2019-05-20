#!/usr/bin/python
# coding:utf-8
__author__ = 'HuangZhi'

import os
import sys

import json

if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.abspath(sys.argv[0])) + '/..')


def decodeSegment(seg, *params, **flags):
    o = json.loads(seg)
    vs = []
    for p in params:
        ns = p.split('/')
        v = o
        for n in ns:
            if n:
                if isinstance(v, list):
                    v = v[int(n)]
                elif isinstance(v, dict):
                    v = v[n]
        vs.append(v)
    fmt = flags.get('format', '\t'.join(['%s'] * len(vs)))

    return fmt % tuple(vs)


def decode(*params, **flags):
    input = sys.stdin
    fin = flags.get('input')
    if fin:
        input = open(fin)

    output = sys.stdout
    fout = flags.get('output')
    if fout:
        output = open(fout, 'w')

    if flags.get('mode') == 'line':
        while True:
            s = input.readline()
            if len(s) == 0:
                break
            s = decodeSegment(s, *params, **flags)
            output.write(s + '\n')
    else:
        s = input.read()
        if len(s) > 0:
            s = decodeSegment(s, *params, **flags)
            output.write(s + '\n')

    if fin:
        input.close()
    if fout:
        output.close()


# main procedure

def showHelp(*params, **flags):
    print 'USAGE: %s [flags] command [params]' % (os.path.basename(sys.argv[0]), )
    print 'COMMANDS:'
    print '  decode node_name1 [node_name2 ...]:'
    print '    -input=file_name: input file name, default is stdin'
    print '    -output=file_name: output file name, default is stdout'
    print '    -mode=normal|line: input mode'
    print '    -format=string: output format'


if __name__ == '__main__':
    cmd = None
    flags = {}
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
    elif cmd == 'decode':
        decode(*params, **flags)
    else:
        print 'Unkown command - %s' % cmd
