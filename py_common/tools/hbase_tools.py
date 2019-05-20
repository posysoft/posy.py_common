#!/usr/bin/python
# coding:utf-8
__author__ = 'HuangZhi'

import os
import sys

import time
import json
import binascii

if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.abspath(sys.argv[0])) + '/..')
from framework.utilities.hbase_wrapper import HbaseConnPool


def scanCells(*params, **flags):
    for c in _Scanner(*params, **flags).iterData():
        yield c


def dumpCells(*params, **flags):
    _Scanner(*params, **flags).scanData()


class _Scanner():
    def __init__(self, *params, **flags):
        self.flags = flags
        self.params = params
        self.files = {}

    def writeRecord(self, cell):
        file_name = self.flags.get('output', '')
        if file_name:
            file_name = file_name % dict([(n, cell[n]) for n in ['key', 'cf', 'c', 'ts', 'ts_d', 'ts_t', 'ts_dt', 'index']])
        f = sys.stdout
        if file_name not in self.files:
            if file_name:
                f = open(file_name, 'wb')
            self.onFileOpen(f)
            self.files[file_name] = 0
        else:
            if file_name:
                f = open(file_name, 'ab')
        index_in_file = self.files[file_name]

        self.onWriteCell(f, cell, index_in_file)
        self.files[file_name] += 1
        if file_name:
            f.close()

    def onWriteCell(self, f, cell, index_in_file):
        out_format = self.flags.get('format')
        if out_format == 'raw':
            if index_in_file:
                f.write('\n')
            f.write(cell['val'])
        elif out_format == 'json':
            fields = ['key', 'col', 'val', 'ts']
            if 'local_ts' in self.flags:
                fields.append('time')
            f.write(',\n' if index_in_file else '\n')
            f.write(json.dumps(dict([(n, cell[n]) for n in fields])))
        else:
            if 'no_data' in self.flags:
                f.write(('%(index)s\t%(key)s\t%(col)s\t%(' + ('time' if 'local_ts' in self.flags else 'ts') + ')s\n') % cell)
            else:
                f.write(('%(index)s\t%(key)s\t%(col)s\t%(val)s\t%(' + ('time' if 'local_ts' in self.flags else 'ts') + ')s\n') % cell)

    def onFileOpen(self, f):
        if self.flags.get('format') == 'json':
            f.write('[')

    def onFileClose(self, f, count):
        if self.flags.get('format') == 'json':
            f.write('\n]')

    def closeFiles(self):
        for file_name, count in self.files.items():
            f = sys.stdout
            if file_name:
                f = open(file_name, 'ab')
            self.onFileClose(f, count)
            if file_name:
                f.close()

    def scanData(self):
        for c in self.iterData():
            self.writeRecord(c)

    def iterData(self):
        if len(self.params) < 1:
            print 'missing table name'
            return
        table = self.params[0]
        start_key = self.params[1] if len(self.params) > 1 else None
        stop_key = self.params[2] if len(self.params) > 2 else None
        columns = self.flags['columns'].split(',') if 'columns' in self.flags else None
        start_ts = self._str2ts(self.flags['start_time']) if 'start_time' in self.flags else 0L
        start_ts = int(self.flags['start_ts']) if 'start_ts' in self.flags else start_ts
        stop_ts = self._str2ts(self.flags['stop_time']) if 'stop_time' in self.flags else (sys.maxint * 1000L)
        stop_ts = int(self.flags['stop_ts']) if 'stop_ts' in self.flags else stop_ts
        snapshot_ts = self._str2ts(self.flags['snapshot_time']) if 'snapshot_time' in self.flags else None
        snapshot_ts = int(self.flags['snapshot_ts']) if 'snapshot_ts' in self.flags else snapshot_ts
        count = int(self.flags.get('count', '-1'))
        filter_str = 'KeyOnlyFilter()' if 'no_data' in self.flags else ''
        filter_str += ((' AND ' if filter_str else '') + 'FirstKeyOnlyFilter()') if 'first_cell' in self.flags else ''
        filter_str += ((' AND ' if filter_str else '') + ('PrefixFilter(\'%s\')' % self.flags['prefix'])) if 'prefix' in self.flags else ''
        filter_str += ((' AND ' if filter_str else '') + ('ColumnPrefixFilter(\'%s\')' % self.flags['col_prefix'])) if 'col_prefix' in self.flags else ''
        filter_str += ((' AND ' if filter_str else '') + ('PageFilter(%d)' % count)) if 'count' in self.flags else ''

        index = 0
        with HbaseConnPool(self.flags['h'], self.flags['p']).fetchConnection() as hb:
            for c in hb.scan(table, by_cell=True, timestamp=snapshot_ts, startRow=start_key, stopRow=stop_key,
                             columns=columns, filterString=filter_str if filter_str else None):
                if index == count:
                    break
                if not (start_ts <= c[3] < stop_ts):
                    continue

                cell = {
                    'key': c[0],
                    'col': c[1],
                    'cf': c[1][:c[1].find(':')],
                    'c': c[1][c[1].find(':')+1:],
                    'val': c[2].encode('hex') if 'hex_value' in self.flags else c[2],
                    'ts': str(c[3]),
                    'ts_dt': '%s.%03d' % (time.strftime('%Y%m%d.%H%M%S', time.localtime(c[3]/1000)), (c[3] % 1000)),
                    'ts_d': time.strftime('%Y%m%d', time.localtime(c[3]/1000)),
                    'ts_t': '%s.%03d' % (time.strftime('%H%M%S', time.localtime(c[3]/1000)), (c[3] % 1000)),
                    'time': '%s.%03d' % (time.strftime('%Y-%m-%d.%H:%M:%S', time.localtime(c[3]/1000)), (c[3] % 1000)),
                    'index': index
                }

                yield cell

                index += 1

        self.closeFiles()

    def _str2ts(self, s):
        ps = s.split('.')
        ts = time.mktime(time.strptime('.'.join(ps[:2]), '%Y-%m-%d.%H:%M:%S')) if len(ps) > 1 else \
            time.mktime(time.strptime(ps[0], '%Y-%m-%d'))
        ts = ts * 1000L + (int(ps[2]) if len(ps) > 2 else 0)
        return ts


def listTables(*params, **flags):
    with HbaseConnPool(flags['h'], flags['p']).fetchConnection() as hb:
        if params:
            cols = hb.list_columns(params[0])
            if 'l' in flags:
                for c in cols.values():
                    print c.name, c
            else:
                for c in cols.keys():
                    print c
        else:
            tables = hb.list_tables()
            for t in tables:
                print t


def setCell(*params, **flags):
    table = params[0] if len(params) > 0 else None
    key = params[1] if len(params) > 1 else None
    column = params[2] if len(params) > 2 else None
    value = params[3] if len(params) > 3 else None
    ts = long(params[4]) if len(params) > 4 else None
    value_file = flags.get('value')
    if not table:
        print 'missing table name'
    if not key:
        print 'missing key'
    if not column:
        print 'missing column name'
    if (value is None) and (not value_file):
        print 'missing value'
    if value_file:
        with open(value_file, 'rb') as f:
            value = f.read()
    with HbaseConnPool(flags['h'], flags['p']).fetchConnection() as hb:
        hb.set(table, key, column, value)


def removeCells(*params, **flags):
    table = params[0] if len(params) > 0 else None
    key = params[1] if len(params) > 1 else None
    columns = params[2] if len(params) > 2 else None
    cf = flags.get('cf', '').rstrip(':')
    if not table:
        print 'missing table name'
    if not key:
        print 'missing key'
    columns = columns.split(',') if columns else []
    with HbaseConnPool(flags['h'], flags['p']).fetchConnection() as hb:
        hb.remove_mc(table, key, columns, cf)


def importCells(*params, **flags):
    table = params[0] if len(params) > 0 else None
    input_file = flags.get('input')
    if not table:
        print 'missing table name'
    f = open(input_file, 'r') if input_file else sys.stdin
    content = f.read()
    f.close()
    cells = json.loads(content)
    with HbaseConnPool(flags['h'], flags['p']).fetchConnection() as hb:
        for c in cells:
            hb.set(table, c['key'], c['col'], binascii.a2b_hex(c['val']) if 'hex_value' in flags else c['val'])


# main procedure

def showHelp(*params, **flags):
    print 'USAGE: %s [flags] command [params]' % (os.path.basename(sys.argv[0]), )
    print 'COMMANDS:'
    print '  help: show this help screen'
    print '  list: list tables'
    print '  list table_name: list columns in the table'
    print '    -l: show detail information of the columns'
    print '  scan table_name [start_key [stop_key]]: show cells in the table'
    print '    -count=row_count: count of records to be returned'
    print '    -no_data: do not fetch cell data from server'
    print '    -hex_value: encode value data in hex format'
    print '    -first_cell: return just one cell for each key'
    print '    -prefix=key_prefix: return the keys that matches the prefix'
    print '    -col_prefix=col_prefix: return the columns that matches the prefix'
    print '    -columns=cf1,cf2,c3,c4...: return the cells in the list of '
    print '                               columns or families'
    print '    -start_ts=timestamp: omit cells before the timestamp'
    print '    -start_time=time: omit cells before the time, in YYYYMMDD.HHMMSS.mmm format'
    print '    -stop_ts=timestamp: omit cells on or after the timestamp'
    print '    -stop_time=time: omit cells on or after the time,'
    print '                     in YYYYMMDD.HHMMSS.mmm format'
    print '    -snapshot_ts=timestamp: get history version before the timestamp'
    print '                            in YYYYMMDD.HHMMSS.mmm format'
    print '    -snapshot_time=time: get history version before the time,'
    print '    -local_ts: show timestamps in readable format'
    print '    -format:output_format: output result in specific format:'
    print '        json: output result as list of objects in json format'
    print '        raw: output raw data of value field in the cells'
    print '        (default): output result as readable text'
    print '    -output=pattern_of_file_name: output result to specified file, '
    print '            data of cells could be dumped to a single file or seperate files.'
    print '            this flag can be a specified file name or a patten of file names.'
    print '            i.e. "%(cf)s.%(c)s.%(key)s.%(ts)s.txt".'
    print '        pattern fields:'
    print '            key: key of the cell'
    print '            cf: column family of the cell'
    print '            c: column name (excluding family) of the cell'
    print '            ts: timestamp of the cell, in number format'
    print '            ts_d: date of timestamp of the cell, in YYYYMMDD format'
    print '            ts_t: daytime of the timestamp of the cell, in HHMMSS.mmm format'
    print '            ts_dt: timestamp of the cell, in YYYYMMDD.HHMMSS.mmm format'
    print '            index: index of record in the result set'
    print '        (default): show result in console'
    print '  set table_name key column [value]: set the value of the cell'
    print '    -value=file_name: read value from the file'
    print '  remove table_name key columns: remove the cell(s)'
    print '    -cf=column_family: family of the columns'
    print '  import table_name: import cells from input or file'
    print '    -input=file_name: read cell content from the file'
    print '    -hex_value: decode value data in hex format'
    print '    -format=input_format: specify the format of the input'
    print '        json: decode the input in json format, (default)'
    print 'COMMON FLAGS:'
    print '  -h=host: specify the hbase server, current is %s' % (flags['h'],)
    print '  -p=port: specify the hbase port, current is %s' % (flags['p'],)

if __name__ == '__main__':
    HBASE_HOST = '127.0.0.1'
    HBASE_PORT = 9090

    cmd = None
    flags = {'h': HBASE_HOST, 'p': str(HBASE_PORT)}
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
    elif cmd == 'list':
        listTables(*params, **flags)
    elif cmd == 'scan':
        dumpCells(*params, **flags)
    elif cmd == 'set':
        setCell(*params, **flags)
    elif cmd == 'remove':
        removeCells(*params, **flags)
    elif cmd == 'import':
        importCells(*params, **flags)
    else:
        print 'Unkown command - %s' % cmd


