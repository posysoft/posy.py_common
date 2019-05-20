# coding:utf-8
__author__ = '. at 13-7-19'

import hashlib, glob, os, types
import time
import random
from StringIO import StringIO
from PIL import Image
import json
import urllib2
import math


# 做md5运算，返回一个32位16进制表示
def md5_hash(s):
    md5 = hashlib.md5(s)
    return md5.hexdigest()


def str2unicode(s):
    if not isinstance(s, unicode):
        return s.decode('utf-8')
    return s


# 计算字符串长度（ascii算1个字符，其他算2个字符）
def ustring_length(s):
    """
    s : unicode string
    """
    count = 0
    for c in s:
        if ord(c) > 127:
            count += 2
        else:
            count += 1
    return count


# generate unique 64 bits numbers
class UniqueNumber(object):
    def __init__(self, upset=False, rand_width=32):
        self.last_num = 0
        self.last_sec = 0
        self.upset = upset
        self.rand_base = int(math.pow(2, rand_width))

    def generate_number(self):
        sec = int(time.time()) * self.rand_base
        if sec != self.last_sec:
            self.last_num = random.randint(0, self.rand_base) if self.upset else 0
            self.last_sec = sec
        else:
            self.last_num = (self.last_num + 1) % self.rand_base

        n = sec + self.last_num

        if self.upset:
            b = bin(n)[2:].zfill(64)
            n = int(b[0::4]+b[1::4]+b[2::4]+b[3::4], 2)
        return n

    def generate_hex(self):
        return self.to_hex(self.generate_number())

    @staticmethod
    def to_hex(number):
        return '%016x' % number

# 计算今天0是起始秒数
def get_day_start_sec(local_time=None, tz=28800):
    """
    :param local_time: timestamp or time struct
    :param tz: int time zone of second
    :return: int seconds of the day start
    """
    if local_time is None:
        i = int(time.time())
    elif isinstance(local_time, (int, long, float)):
        i = int(local_time)
    else:
        i = int(time.mktime(local_time))
    return i - ((i + tz) % 86400)


# 周期时间计算
def get_nearest_week_time(wday, hour, min=0, sec=0):
    """
    hour 0-23
    min 0-59
    sec 0-59
    wday 1-7
    """
    # 按星期为周期
    now_time = time.localtime()
    wday -= 1
    dif_day = wday - now_time.tm_wday
    if dif_day < 0:
        dif_day += 7

    dif_hour = hour - now_time.tm_hour
    if dif_hour < 0:
        if not dif_day:
            dif_day += 7
    dif_min = min - now_time.tm_min
    if dif_min < 0:
        if not dif_day and not dif_hour:
            dif_day += 7
    dif_sec = sec - now_time.tm_sec
    if dif_sec < 0:
        if not dif_day and not dif_hour and not dif_min:
            dif_day += 7
    day_sec = dif_day * 24 * 60 * 60 + dif_hour * 60 * 60 + dif_min * 60 + dif_sec
    t_s = time.mktime(now_time) + day_sec
    return int(t_s)


def get_nearest_week_time2(wday, hour, min=0, sec=0, p=0):
    nt = time.localtime()
    wst = time.mktime(nt) - nt.tm_wday * 24 * 60 * 60 - nt.tm_hour * 60 * 60 - nt.tm_min * 60 - nt.tm_sec
    wday -= 1
    period = p * (7 * 24 * 60 * 60) + wday * 24 * 60 * 60 + hour * 60 * 60 + min * 60 + sec
    return int(wst + period)


# 获取对象路径下的值
def get_path_value(subject, path, default=None, getter=None):
    _t = subject
    v = None
    for p in path:
        if getter:
            v = getter(_t, p)
        else:
            try:
                v = _t[p]
            except:
                v = None
        if v is not None:
            _t = v
        else:
            return default
    return v if v is not None else default


def set_path_value(subject, path, value, creator=dict, getter=None, setter=None):
    _t = subject
    v = None
    for p in path[:-1]:
        if getter:
            v = getter(_t, p)
        else:
            try:
                v = _t[p]
            except:
                v = None
        if v is None:
            v = creator()
            if setter:
                setter(_t, p, v)
            else:
                _t[p] = v
        _t = v
        v = None
    p = path[-1]
    if setter:
        setter(_t, p, value)
    else:
        _t[p] = value


def region_binary_search(sorted_list, target, cmp_pos=-1, value=True):
    """
    给出指定数值，查找改数值出现在哪个区间
    sorted_list : list of dict, contain the compare field
    target : int 需要判断的目标数值
    cmp_pos : int 区间比较数值在list的下标
    """
    length = len(sorted_list)
    start = 0
    end = length - 2
    while start <= end:
        mid = (start + end) / 2
        t = sorted_list[mid]
        if target < t[cmp_pos]:
            end = mid - 1
        elif target >= sorted_list[mid + 1][cmp_pos]:
            start = mid + 1
        else:
            if value:
                return t
            return mid
    if value:
        return sorted_list[start]
    return start


def import_modules(path):
    name_prefix = '.'.join([x for x in path.split('/') if x])
    modules = {}
    for file in glob.glob(path +"/*.py"):
        file = os.path.basename(file)[:-3]
        m = __import__(name_prefix+'.'+file, fromlist=['*'])
        modules[file] = m
    return modules


def get_class_from_module(modules, class_name):
    if type(modules) is types.DictionaryType:
        for n, m in modules.items():
            r = get_class_from_module(m, class_name)
            if r:
                return r
    elif (type(modules) is types.ListType) or (type(modules) is types.TupleType):
        for m in modules:
            r = get_class_from_module(m, class_name)
            if r:
                return r
    else:
        try:
            r = getattr(modules, class_name)
            return r
        except:
            return None


# import class from class path
def import_object(obj_path):
    module_path, obj_name = obj_path.rsplit('.', 1)
    module = __import__(module_path, globals(), locals(), ['from'])
    return getattr(module, obj_name)


# 按照概率从列表中随机选择一项
def selectRandom(choices, max_prob=None, prob_pos=0):
    if len(choices) == 0:
        return None
    if max_prob is None:
        max_prob = 0
        for c in choices:
            try:
                max_prob += c[prob_pos]
            except:
                pass

    if max_prob == 0:
        return random.choice(choices)

    r = random.randint(0, max_prob)

    for c in choices:
        try:
            r -= c[prob_pos]
        except:
            pass
        if r < 0:
            return c
    return None

# file types


class ImageIdentifier:
    _FORMATS = (
        ('\xff\xd8', '\xff\xd9', 'image/jpeg'),
        ('\x89\x50\x4E\x47\x0D\x0A\x1A\x0A', None, 'image/png'),
        ('GIF89a', None, 'image/gif'),
        ('GIF87a', None, 'image/gif'),
        ('BM', None, 'image/bmp'),
        ('\x4d\x4d', None, 'image/tiff'),
        ('\x49\x49', None, 'image/tiff'),
    )

    @staticmethod
    def identifyImageFormat(content):
        for fmt in ImageIdentifier._FORMATS:
            if fmt[0] and (content[: len(fmt[0])] != fmt[0]):
                    continue
            if fmt[1] and (content[-len(fmt[1]):] != fmt[1]):
                    continue
            return fmt[2]
        return None


class FileExtIdentifier:
    _FORMATS = {
        'js': 'application/x-javascript',
        'css': 'text/css',
        'html': 'text/html',
        'htm': 'text/html',
        'txt': 'text/plain',
        'log': 'text/plain',
        'ico': 'image/ico',
        'jpg': 'image/jpeg',
        'png': 'image/png',
        'gif': 'image/gif',
        'bmp': 'image/bmp',
        'pdf': 'application/pdf',
        'svg': 'image/svg+xml'
    }

    @staticmethod
    def identifyExtFormat(file_name):
        parts = file_name.split('/')
        if parts:
            parts = parts[-1].split('.')
            if parts:
                return FileExtIdentifier._FORMATS.get(parts[-1], None)
        return None


class ImageTool(object):
    """
    image tool
    """
    def get_url_img(self, url):
        cnt, cnt_ty = self.http_get(url)
        f = StringIO(cnt)
        img = Image.open(f)
        return img, cnt_ty

    @staticmethod
    def http_get(url, data=None):
        if data is not None and not isinstance(data, basestring):
            data = json.dumps(data, ensure_ascii=False)
        req = urllib2.Request(url, data=data)
        res = urllib2.urlopen(req)
        if 200 != res.code:
            raise Exception('response code %s error' % res.code)
        cnt = res.read()
        cnt_ty = res.headers.type
        if not cnt_ty.startswith('image'):
            raise Exception('It is not a image url %s' % url)
        return cnt, cnt_ty

    # 增加图片底框
    @staticmethod
    def paste_to_bottom_frame(img, frame_size=(900, 500), frame_mode='RGB', frame_color=(255, 255, 255)):
        """
        :param img: Image object
        :param frame_size:
        :param frame_mode:
        :param frame_color:
        :return:
        """
        bf_img = Image.new(frame_mode, frame_size, frame_color)
        width, high = frame_size
        x, y = img.size
        rw = width / float(x)
        rh = high / float(y)
        r = min(rw, rh)
        resize = map(int, [x * r, y * r])
        img = img.resize(resize, Image.ANTIALIAS)
        of_x = max((width - resize[0]) / 2, 0)
        of_y = max((high - resize[1]) / 2, 0)
        bf_img.paste(img, (of_x, of_y))
        return bf_img