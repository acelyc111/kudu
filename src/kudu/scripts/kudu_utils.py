#! /usr/bin/env python
# coding=utf-8

import datetime
import dateutil.relativedelta
import logging
from logging.handlers import RotatingFileHandler
import os
import requests

LOG = logging.getLogger()
g_time = datetime.datetime.now()


def init_log():
    if not os.path.exists('log/'):
        os.makedirs('log')
    handler = RotatingFileHandler('log/kudu.log',
                                  mode='a',
                                  maxBytes=100*1024*1024,
                                  backupCount=10)
    handler.setFormatter(
        logging.Formatter(
            fmt='%(asctime)s [%(thread)d] [%(levelname)s] %(filename)s:%(lineno)d %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'))
    LOG.addHandler(handler)
    LOG.setLevel(logging.INFO)


def make_dir(path):
    try:
        os.mkdir(path)
    except OSError, e:
        if e.errno != os.errno.EEXIST:
            raise
        pass


def script_path():
    return os.path.split(os.path.realpath(__file__))[0]


def get_year(last_month):
    time = g_time
    if last_month:
        time += dateutil.relativedelta.relativedelta(months=-1)
    return time.strftime('%Y')


def get_month(last_month):
    time = g_time
    if last_month:
        time += dateutil.relativedelta.relativedelta(months=-1)
    return time.strftime('%m')


def prepare_pricing_month_path(last_month=False):
    month_base_path = script_path() + '/year=' + get_year(last_month)
    make_dir(month_base_path)
    month_base_path += '/month=' + get_month(last_month)
    make_dir(month_base_path)
    data_path = month_base_path + '/data'
    make_dir(data_path)
    return month_base_path + '/', data_path + '/'


def get_year_month(last_month):
    return get_year(last_month) + '-' + get_month(last_month)


def get_date():
    time = g_time
    return time.strftime('%Y-%m-%d')


def get_date_list(start, end, step=1, format="%Y-%m-%d"):
    strptime, strftime = datetime.datetime.strptime, datetime.datetime.strftime
    days = (strptime(end, format) - strptime(start, format)).days
    return [strftime(strptime(start, format) + datetime.timedelta(i), format) for i in xrange(0, days, step)]


# method:
#   append: 追加写入账单
#   reload: 清空数据并重新写入账单
def upload_usage_data(method, filename):
    LOG.info('Start to report %s by %s mode' % (filename, method))
    report_url = "http://production-cost.api.xiaomi.net/api/v1/data/upload"
    token = "c2534683e5504ab4850c49873a36de61"
    url = "%s?sk=%s&method=%s" % (report_url, token, method)
    with open(filename, "rb") as f:
        resp = requests.post(url, files={"file": f})
        if resp.status_code == 200:
            # 成功
            details = resp.json()
            if details['code'] == 0:
                LOG.info('Succeed to report %s by %s mode' % (filename, method))
            else:
                LOG.fatal('Failed to report %s by %s mode, details: %s' % (filename, method, str(details).decode("unicode-escape")))
        else:
            # 失败
            LOG.fatal('Report failed, code %d' % resp.status_code)


g_script_path = script_path()
os.environ['KUDU_CONFIG'] = g_script_path
init_log()
