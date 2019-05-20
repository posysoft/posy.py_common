# coding:utf-8
__author__ = 'HuangZhi'


import os
import sys
import glob
import urllib2
#os.chdir(os.path.dirname(os.path.abspath(sys.argv[0])))
import time
import json
import math
from PIL import Image
from cStringIO import StringIO

PAI = 3.14159265359


def _calcTransScale(size, *params, **flags):
    crop_x = flags.get('crop_x', '0').strip()
    crop_x = int((size[0]*float(crop_x[:-1])/100) if crop_x[-1] == '%' else crop_x)
    crop_x = crop_x if crop_x >= 0 or 'wrap_coords' not in flags else (size[0] + crop_x)
    crop_y = flags.get('crop_y', '0').strip()
    crop_y = int((size[1]*float(crop_y[:-1])/100) if crop_y[-1] == '%' else crop_y)
    crop_y = crop_y if crop_y >= 0 or 'wrap_coords' not in flags else (size[1] + crop_y)
    crop_width = flags.get('crop_width', '100%').strip()
    crop_width = int((size[0]*float(crop_width[:-1])/100) if crop_width[-1] == '%' else crop_width)
    crop_width = crop_width if crop_width >= 0 or 'wrap_coords' not in flags else (size[0] + crop_width)
    crop_height = flags.get('crop_height', '100%').strip()
    crop_height = int((size[1]*float(crop_height[:-1])/100) if crop_height[-1] == '%' else crop_height)
    crop_height = crop_height if crop_height >= 0 or 'wrap_coords' not in flags else (size[1] + crop_height)

    mode = flags.get('resize', 'fit_size')
    ratio = 1.0
    if mode == 'fix_scale':
        ratio = float(flags.get('scale', 1.0))
    else:   # fit_size
        r_max_size = min(float(flags.get('max_width', 1000000000))/crop_width, float(flags.get('max_height', 1000000000))/crop_height)
        r_min_size = max(float(flags.get('min_width', 0))/crop_width, float(flags.get('min_height', 0))/crop_height)
        ratio = max(r_max_size, r_min_size, float(flags.get('min_scale', 0)))
        ratio = min(ratio, float(flags.get('max_scale', 1000000000)))

    crop_incline = float(flags.get('crop_incline', 0.0))
    incline_offset = [0, 0]
    incline_size = [0, 0]
    if crop_incline != 0.0:
        angle = crop_incline * PAI / 180.0
        sin_a = math.sin(angle)
        cos_a = math.cos(angle)
        offset_rt = [crop_width * cos_a, -crop_width * sin_a]
        offset_lb = [crop_height * sin_a, crop_height * cos_a]
        offset_rb = [offset_rt[0] + offset_lb[0], offset_rt[1] + offset_lb[1]]
        offset_rect = [
            int(min(0, offset_rt[0], offset_lb[0], offset_rb[0])),
            int(min(0, offset_rt[1], offset_lb[1], offset_rb[1])),
            int(max(0, offset_rt[0], offset_lb[0], offset_rb[0])),
            int(max(0, offset_rt[1], offset_lb[1], offset_rb[1])),
        ]
        incline_offset = [crop_x + offset_rect[0], crop_y + offset_rect[1]]
        incline_size = [offset_rect[2] - offset_rect[0], offset_rect[3] - offset_rect[1]]

    return (crop_x, crop_y), (crop_width, crop_height),  (int(crop_width * ratio), int(crop_height * ratio)), \
        crop_incline, incline_offset, incline_size


def transformImages(*params, **flags):
    input_pattern = params[0] if len(params) > 0 else None
    output_pattern = params[1] if len(params) > 1 else ('gray.%(name)s.jpg' if 'gray' in flags else 'resize.%(name)s.jpg')

    count = 0
    fails = []
    flags['_count'] = count
    flags['_fails'] = fails

    for input_name in glob.glob(input_pattern):
        count += 1
        try:
            f_name = os.path.basename(input_name)
            name_parts = f_name.split('.')
            f_title = '.'.join(name_parts[:(-1 if len(name_parts) > 1 else None)])
            f_ext = name_parts[-1] if len(name_parts) > 1 else ''
            f_info = {'name': f_name, 'title': f_title, 'ext': f_ext}
            output_name = output_pattern % f_info
            input = open(input_name, 'rb')
            output = open(output_name, 'wb')

            flags['_input_name'] = input_name
            flags['_output_name'] = output_name
            transformImage(input, output, **flags)
            input.close()
            output.close()
        except BaseException, e:
            if __name__ == '__main__':
                if 'silent' not in flags:
                    print '%s failed:' % input_name, e
                fails.append(input_name)

    if 'silent' not in flags:
        print 'FINISH: %d records processed with %d fails' % (count, len(fails))
        for f in fails:
            print '  Fail: %s' % f


def transformImage(f_input, f_output, **flags):
    input_image = Image.open(f_input)
    input_size = input_image.size
    crop_offset, crop_size, output_size, crop_incline, incline_offset, incline_size = \
        _calcTransScale(input_size, **flags)
    if crop_incline == 0.0:
        output_image = input_image.crop((crop_offset[0], crop_offset[1],
                                         crop_offset[0]+crop_size[0], crop_offset[1]+crop_size[1]))
    else:
        output_image = input_image.crop((incline_offset[0], incline_offset[1],
                                         incline_offset[0]+incline_size[0], incline_offset[1]+incline_size[1]))
        output_image = output_image.rotate(-crop_incline, Image.BICUBIC, True)
        output_image = output_image.crop(((output_image.size[0]-crop_size[0]) / 2,
                                        (output_image.size[1]-crop_size[1]) / 2,
                                        (output_image.size[0]+crop_size[0]) / 2,
                                        (output_image.size[1]+crop_size[1]) / 2))
    output_image = output_image.resize(output_size, Image.ANTIALIAS)

    if 'gray' in flags:
        output_image = output_image.convert('L')
    elif output_image.mode == 'P':
        output_image = output_image.convert('RGB')

    output_image.save(f_output, 'JPEG')
    if __name__ == '__main__' and 'silent' not in flags:
        print "%s(%dx%d) [(%d,%d),(%dx%d),%.1f] -> %s(%dx%d%s)" % (flags['_input_name'], input_size[0], input_size[1],
                                    crop_offset[0], crop_offset[1], crop_size[0], crop_size[1], crop_incline,
                                    flags['_output_name'], output_size[0], output_size[1],
                                    ', grayed' if 'gray' in flags else '')


# main procedure

def showHelp(*params, **flags):
    print 'USAGE: %s [flags] command [params]' % (os.path.basename(sys.argv[0]), )
    print 'COMMANDS:'
    print '  help: show this help screen'
    print '  trans input output: transform input to output'
    print '                      i.e. trans img/*.jpg resize/%(name)s.jpg'
    print '    -resize=mode: algorithm for resize'
    print '        fit_size: fit the image into specified size (default)'
    print '        fix_scale: scale the image with specified ratio'
    print '      -max_width=pixels: max width in fit_size mode'
    print '      -max_height=pixels: max height in fit_size mode'
    print '      -min_width=pixels: min width in fit_size mode'
    print '      -min_height=pixels: min height in fit_size mode'
    print '      -min_scale=scale: min scale ratio in fit_size mode'
    print '      -max_scale=scale: max scale ratio in fit_size mode'
    print '      -scale=scale: scale ratio in fix_scale mode'
    print '    -crop_x=x: coordinate x of left-top point of crop region'
    print '    -crop_y=y: coordinate y of left-top point of crop region'
    print '    -crop_width=width: width of crop region'
    print '    -crop_height=height: height of crop region'
    print '    -crop_incline=degrees: incline of crop region, in degrees'
    print '    -gray: transform the image to gray scale mode'
    print 'COMMON FLAGS:'
    print '  -silent: run in silent mode'


if __name__ == '__main__':
    RESIZE_MODE = 'fit_size'
    MIN_WIDTH = 300
    MIN_HEIGHT = 300
    MAX_WIDTH = 800
    MAX_HEIGHT = 800
    MIN_SCALE = 0.0
    MAX_SCALE = 1.0
    SCALE = 1.0

    cmd = None
    flags = {'resize': RESIZE_MODE,
             'min_width': MIN_WIDTH, 'min_height': MIN_HEIGHT, 'max_width': MAX_WIDTH, 'max_height': MAX_HEIGHT,
             'min_scale': MIN_SCALE, 'max_scale': MAX_SCALE, 'scale': SCALE}
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
    elif cmd == 'trans':
        transformImages(*params, **flags)
    else:
        print 'Unkown command - %s' % cmd
