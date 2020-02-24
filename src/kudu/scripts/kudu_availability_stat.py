#! /usr/bin/env python
# coding=utf-8

# A tool for availability statistics of kudu clusters

# Usage example:
# ./kudu_availability_stat.py 2020-02-02 2020-03-01 # for all prc clusters
# ./kudu_availability_stat.py 2020-02-02 2020-03-01 c3tst-master # for a specific cluster

import commands
import datetime
import os
import re
import sys
import time
import kudu_utils
import yaml

g_clusters_info_dict = yaml.load(open(kudu_utils.g_script_path + '/kudurc', 'r').read(), Loader=yaml.FullLoader)
g_clusters_info = g_clusters_info_dict['clusters_info']
g_total_count = 0
g_success_count = 0

def calc_availability_for_cluster(cluster_name, start, end):
    kudu_utils.LOG.info('Start to collect availability statistics for cluster %s' % cluster_name)
    global g_total_count, g_success_count
    monitor_table = 'system.monitor'
    # Output: (int32 total_count=xxx, int32 success_count=xxx)
    cmd = '%s/kudu table scan @%s %s -columns=total_count,success_count ' \
          '-predicates=[\\"AND\\",[\\"\\>=\\",\\"key\\",%s],[\\"\\<\\",\\"key\\",%s]] | grep "total_count"' \
          % (kudu_utils.g_script_path, cluster_name, monitor_table, start, end)
    status, output = commands.getstatusoutput(cmd)
    if status != 0:
        kudu_utils.LOG.fatal('Unable to execute "kudu table scan": %s', output)
        return 0.0
    total_count = 0
    success_count = 0
    for line in output.splitlines():
        match_obj = re.search(r'int32 total_count=([0-9]+), int32 success_count=([0-9]+)', line, re.M | re.I)
        if match_obj:
            total_count += int(match_obj.group(1))
            success_count += int(match_obj.group(2))
        else:
            kudu_utils.LOG.error('Table %s value format error, line\n%s' % (monitor_table, line))
    kudu_utils.LOG.info('total_count: %s, success_count: %s' % (total_count, success_count))
    g_total_count += total_count
    g_success_count += success_count
    if total_count == 0:
        availability = 0.0
    else:
        availability = float(success_count)/total_count
    return availability


def calc_availability_for_clusters(cluster_name_list, start_date, end_date):
    start_ts = int(time.mktime(datetime.datetime.strptime(start_date, "%Y-%m-%d").timetuple()))
    end_ts = int(time.mktime(datetime.datetime.strptime(end_date, "%Y-%m-%d").timetuple()))
    print("%-30s%-12s" % ("cluster", "availability"))
    for cluster_name in cluster_name_list:
        availability = calc_availability_for_cluster(cluster_name, start_ts, end_ts)
        print("%-30s%-12.6f" % (cluster_name, availability))
    if g_total_count == 0:
        total_availability = 0.0
    else:
        total_availability = float(g_success_count)/g_total_count
    print("%-30s%-12.6f" % ("total", total_availability))


def main(argv=None):
    if argv is None:
        argv = sys.argv

    cluster_name_list = []
    if len(argv) == 3:
        # Calculate all clusters except public_share cluster
        for cluster in g_clusters_info.iterkeys():
            cluster_info = g_clusters_info[cluster]
            if cluster_info['charge_type'] == 'public_share':
                kudu_utils.LOG.warning('Ignore public_share cluster %s' % cluster)
            else:
                cluster_name_list.append(cluster)
    elif len(argv) == 4:
        # Calculate specified cluster
        cluster_name_list.append(argv[3])
    else:
        kudu_utils.LOG.fatal('Usage: %s <start_date> <end_date> [cluster_name]' % argv[0])
        return

    start_date = argv[1]
    end_date = argv[2]
    calc_availability_for_clusters(cluster_name_list, start_date, end_date)


if __name__ == "__main__":
    main()
