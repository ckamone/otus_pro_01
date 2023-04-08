#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '  
#                     '$request_time';

import gzip
import os
from collections import namedtuple
import re
from datetime import datetime
from statistics import median
import argparse
import json

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log"
}





# функция которая ищет последний лог удобно возвращать namedtuple
def get_last_logfile(mypath):
    f = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        for i in filenames:
            if '-ui.' in i:
                f.append(i)
        break
    if len(f):
        f.sort()
        last_log_name = f[-1]
        Logfile = namedtuple("Logfile",'path name date is_gz')
        date = re.findall(r'\d*\.\d+|\d+',last_log_name)
        date_obj = datetime.strptime(date[0], '%Y%m%d')
        last_log_obj = Logfile(mypath, last_log_name, date_obj, '.gz' in last_log_name)
        return last_log_obj
    else:
        return None


# функция парсер лога 
def parser(gen):
    temp = {
        'total_count':0,
        'total_request_time':0
        }
    for i in gen:
        url = re.findall(r'\w+\s[/][^\s]+',i)
        if len(url) != 0:
            url = url[0].split(' ')[1]
            req_time = float(i.split(' ')[-1])
            if not temp.get(url):
                temp[url]=[req_time]
                temp['total_count']+=1
                temp['total_request_time']+=req_time
            else:
                temp[url].append(float(i.split(' ')[-1]))
                temp['total_count']+=1
                temp['total_request_time']+=req_time
    return temp


def genf(file_content):
    file_content = file_content.decode(encoding='utf-8')
    for i in file_content.split('\n'):
        yield i


#считаем статистику
def counter(d: dict, limit):
    result = []
    for k,v in d.items():
        if k not in ['total_count', 'total_request_time']:
            result.append({
                "url":k,
                "count":len(v),
                "count_perc":round(len(v)/d['total_count']*100,3),
                "time_sum":sum(v),
                "time_perc":round(sum(v)/d['total_request_time']*100,3),
                "time_avg":sum(v)/len(v),
                "time_max":max(v),
                "time_med":median(v),
            })
    result = sorted(result, key=lambda x: x['time_sum'], reverse=True)
    return result[:limit]


#функция создатель отчета
def render_report(stats, path, file_date):
    with open('report.html', 'r') as f:
        text = f.read()

    text = text.replace('$table_json', str(stats))
    rep_name = os.path.join(path, f'report-{file_date.strftime("%Y.%m.%d")}.html')
    with open(rep_name, 'w') as f:
        f.write(text)


def run(config):
    last_logfile = get_last_logfile(config['LOG_DIR'])
    if last_logfile is not None:
        path = os.path.join(last_logfile.path, last_logfile.name)
        read_cmd = "gzip.open(path, 'rb')" if last_logfile.is_gz else "open(path, 'rb')" # тернарный оператор
        with eval(read_cmd) as f:
            lfile = f.read()
        generator = genf(lfile)
        parse_data = parser(generator)
        stats = counter(parse_data, config['REPORT_SIZE'])
        render_report(stats, config['REPORT_DIR'], last_logfile.date)

    else:
        pass # TODO logging that no files


def main(config):

    parser = argparse.ArgumentParser()

    parser.add_argument('--config',
                        default=config,
                        help="set configuration file path")

    args = parser.parse_args()
    if args.config is not config:
        try:
            with open(args.config, 'r') as f:
                config = json.loads(f.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            raise
    run(config)


if __name__ == "__main__":
    main(config)


# tests, monitoring (logging?)