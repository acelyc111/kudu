#! /usr/bin/env python
# coding=utf-8

import commands
import datetime
from git import Repo
import heapq
import logging
from logging.handlers import RotatingFileHandler
import json
import os
import re
import sys
import time
import kudu_utils
import yaml


g_ignore_db_set = ('system', 'lcsbinlog', 'default', 'zhangxu_test_kudu')
g_month_path, g_month_data_path = kudu_utils.prepare_pricing_month_path()
g_clusters_info_dict = yaml.load(open(kudu_utils.g_script_path + '/kudurc', 'r').read(), Loader=yaml.FullLoader)
g_clusters_info = g_clusters_info_dict['clusters_info']
g_commit_filenames = list()
g_git_repo_dir = ''


def printtsr(level, table, size, reason):
    kudu_utils.LOG.log(level, 'table: ' + table + (', size: %fG' % (size/(1 << 30)) + ', reason: ') + reason)


class TopKHeap(object):
    def __init__(self, k):
        self.k = k
        self.data = []

    def push(self, elem):
        if len(self.data) < self.k:
            heapq.heappush(self.data, elem)
        else:
            top_k_small = self.data[0]
            if elem['size'] > top_k_small['size']:
                heapq.heapreplace(self.data, elem)

    def top_k(self):
        return {x['table']: x['size'] for x in reversed([heapq.heappop(self.data) for _ in xrange(len(self.data))])}


def add_org_size(dbtable, org, size, org_size_desc):
    if len(org) == 0:
        printtsr(logging.WARNING, dbtable, size, 'Org name is empty')
        return False

    if org not in org_size_desc.keys():
        org_size_desc[org] = {}
        org_size_desc[org]['size'] = 0
        org_size_desc[org]['desc'] = TopKHeap(10)
    org_size_desc[org]['size'] += size
    org_size_desc[org]['desc'].push({'size': size, 'table': dbtable})
    return True


def get_org_size_desc_from_olap(cluster_name, dbtable_size_dict, known_db_org_dict):
    db_org_dict = {}
    meta_table = 'system.kudu_table_owners'
    cmd = '%s/kudu table scan @%s %s -show_values=true' \
          ' -columns=name,db,org 2>&1 | grep "(string name=\\\""'\
          % (kudu_utils.script_path(), cluster_name, meta_table)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.error('Scan table %s error, command %s, status %d, output \n%s' % (meta_table, cmd, status, output))
    else:
        for line in output.splitlines():
            match_obj = re.search(r'string name="(.*)", string db="(.*)", string org="(.*)"', line, re.M | re.I)
            if match_obj:
                db = match_obj.group(2)
                org = match_obj.group(3)
                db_org_dict[db] = org
            else:
                kudu_utils.LOG.error('Table %s value format error, line\n%s' % (meta_table, line))

    total_ignored_size = 0.0
    org_size_desc = {}
    for dbtable, size in dbtable_size_dict.iteritems():
        db_table_list = dbtable.split('.')
        if len(db_table_list) != 2:
            total_ignored_size += size
            printtsr(logging.WARNING, dbtable, size, 'Lack db')
            continue

        db, table = db_table_list[0], db_table_list[1]
        if db in g_ignore_db_set:
            total_ignored_size += size
            printtsr(logging.INFO, dbtable, size, 'Ignored table')
            continue

        if db in known_db_org_dict.keys():
            # 'org' from config file
            org = known_db_org_dict[db]
        elif db in db_org_dict.keys():
            # 'org' from system table
            org = db_org_dict[db]
        else:
            total_ignored_size += size
            printtsr(logging.WARNING, db, size, 'Lack org ID')
            continue

        if not add_org_size(dbtable, org, size, org_size_desc):
            total_ignored_size += size
            continue

    printtsr(logging.WARNING, 'TOTAL', total_ignored_size, 'Total ignored size')
    return org_size_desc


def get_cluster_stat_filename(date, cluster_name):
    return g_month_data_path + date + '_' + cluster_name


def get_service_usage_filename(date):
    return g_month_data_path + date + '_kudu_total'


def collect_origin_usage_for_cluster(cluster_name, cluster_info):
    kudu_utils.LOG.info('Start to collect usage info for cluster %s' % cluster_name)
    # Output: db.table size
    cmd = '%s/kudu_collector -collector_master_addrs=%s ' \
          '-collector_report_method=local -collector_metrics=on_disk_size -log_dir=./log | ' \
          'egrep "^table on_disk_size " | sort | awk \'{print $3, $4}\'' \
          % (kudu_utils.g_script_path, cluster_info['master_addresses'])
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.fatal('Table stat error')
        return

    dbtable_size_dict = {}
    for dbtable_size_str in output.splitlines():
        dbtable_size_list = dbtable_size_str.split(' ')
        assert(len(dbtable_size_list) == 2)
        dbtable_size_dict[dbtable_size_list[0]] = float(dbtable_size_list[1])
    known_db_org_dict = {}
    if 'special_db_org' in cluster_info.keys():
        known_db_org_dict = cluster_info['special_db_org']
    org_size_desc = get_org_size_desc_from_olap(cluster_name, dbtable_size_dict, known_db_org_dict)

    results = []
    date = time.strftime('%Y-%m-%d', time.localtime())
    period = int(time.mktime(datetime.datetime.strptime(date, "%Y-%m-%d").timetuple()))
    for org, size_desc in org_size_desc.iteritems():
        result = dict()
        result['period'] = period
        result['service_name'] = 'kudu'
        result['region_name'] = cluster_info['region']
        result['charge_type_name'] = cluster_info['charge_type']
        result['instance_name'] = cluster_info['instance']
        result['cluster'] = cluster_name
        result['account_type'] = 'org' if org.find('CL') != -1 else 'kerberos'
        result['account'] = org
        result['usage'] = size_desc['size']
        result['charge_object'] = size_desc['desc'].top_k()
        results.append(result)
    origin_usage_filename = get_cluster_stat_filename(date, cluster_name)
    with open(origin_usage_filename, 'w') as origin_usage_file:
        json.dump(results, origin_usage_file)
        origin_usage_file.close()

        g_commit_filenames.append(origin_usage_filename)


def get_cluster_info(cluster_name):
    if cluster_name not in g_clusters_info.keys():
        kudu_utils.LOG.fatal('Cluster %s not found' % cluster_name)
        return None

    cluster_info = g_clusters_info[cluster_name]
    if cluster_info['charge_type'] == 'public_share':
        kudu_utils.LOG.warning('Ignore public_share cluster %s' % cluster_name)
        return None

    return cluster_info


def collect_origin_usage_for_clusters(cluster_name_list):
    for cluster_name in cluster_name_list:
        cluster_info = get_cluster_info(cluster_name)
        if not cluster_info:
            continue
        collect_origin_usage_for_cluster(cluster_name, cluster_info)


def calc_usage_result(origin_usage_filename, service_usage_file):
    kudu_utils.LOG.info('Start to process daily statistics file %s' % origin_usage_filename)
    if not os.path.exists(origin_usage_filename):
        kudu_utils.LOG.error('File not exist')
        return
    with open(origin_usage_filename, 'r') as origin_usage_file:
        users_usage = json.load(origin_usage_file)
        for user_usage in users_usage:
            service_usage_file.write('%s, %s, %s, %s, %s, %s, %s, %s, \'{"storage_bytes":%d}\', \'%s\'\n'
                                    % (user_usage['period'],
                                       user_usage['service_name'],
                                       user_usage['region_name'],
                                       user_usage['charge_type_name'],
                                       user_usage['instance_name'],
                                       user_usage['cluster'],
                                       user_usage['account_type'],
                                       user_usage['account'],
                                       user_usage['usage'],
                                       json.dumps(user_usage['charge_object'])))
        origin_usage_file.close()
    kudu_utils.LOG.info('Write to file finished')


def calc_usage_result_for_cluster(service_usage_file, cluster_name, date):
    origin_usage_filename = get_cluster_stat_filename(date, cluster_name)
    calc_usage_result(origin_usage_filename, service_usage_file)


def calc_usage_result_for_clusters(cluster_name_list, date_list):
    for date in date_list:
        service_usage_filename = get_service_usage_filename(date)
        with open(service_usage_filename, 'w') as service_usage_file:
            # Write header
            service_usage_file.write('period, service_name, region_name, charge_type_name, instance_name, '
                              'cluster, account_type, account, usage, charge_object\n')
            for cluster_name in cluster_name_list:
                cluster_info = get_cluster_info(cluster_name)
                if not cluster_info:
                    continue
                calc_usage_result_for_cluster(service_usage_file, cluster_name, date)
            service_usage_file.close()
        kudu_utils.upload_usage_data('append', service_usage_filename)
        g_commit_filenames.append(service_usage_filename)


def push_file_to_repo(filenames):
    repo = Repo(g_git_repo_dir)
    assert not repo.bare

    remote = repo.remote()
    remote.pull()

    index = repo.index
    index.add(filenames)
    index.commit('Kudu add statistics files')

    remote.push()

    kudu_utils.LOG.info('Pushed files %s to repo' % str(filenames))


def main(argv=None):
    if not os.path.exists(g_git_repo_dir + '/.git'):
        kudu_utils.LOG.fatal('You must set `g_git_repo_dir` to a valid directory contains `.git`')
        return

    if argv is None:
        argv = sys.argv

    cluster_name_list = []
    if len(argv) == 1:
        # Calculate all clusters
        cluster_name_list = list(g_clusters_info.iterkeys())
    elif len(argv) == 2:
        # Calculate specified cluster
        cluster_name_list.append(argv[1])
    else:
        kudu_utils.LOG.fatal('Usage: $0 [cluster_name]')
        return

    collect_origin_usage_for_clusters(cluster_name_list)

    # date_list = kudu_utils.get_date_list('2019-06-01', kudu_utils.get_date())
    date_list = [kudu_utils.get_date()]
    calc_usage_result_for_clusters(cluster_name_list, date_list)

    push_file_to_repo(g_commit_filenames)


if __name__ == "__main__":
    main()
