#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
#                     '$status $body_bytes_sent "$http_referer" '
#                     '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '  
#                     '$request_time';

import argparse
import json
import logging
import os
import re
import gzip
from collections import namedtuple
from datetime import datetime
from statistics import median

config = {
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "LOG_FILE": "scr.log"
}

FORMAT = '[%(asctime)s] %(levelname).1s %(message)s'
DATEFORMAT = '%Y.%m.%d %H:%M:%S'

formatter = logging.Formatter(fmt=FORMAT, datefmt=DATEFORMAT)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stream_handler)

if config.get('LOG_FILE', False):
    logging.basicConfig(filename=config['LOG_FILE'])
    file_handler = logging.FileHandler(config['LOG_FILE'])
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


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
        Logfile = namedtuple("Logfile", 'path name date is_gz')
        date = re.findall(r'\d*\.\d+|\d+', last_log_name)
        date_obj = datetime.strptime(date[0], '%Y%m%d')
        last_log_obj = Logfile(mypath, last_log_name, date_obj, '.gz' in last_log_name)
        return last_log_obj
    else:
        return None


# функция парсер лога 
def parser(gen):
    temp = {
        'total_count': 0,
        'total_request_time': 0
    }
    total_entries = 0
    errors = 0
    for i in gen:
        total_entries += 1
        url = re.findall(r'\w+\s[/][^\s]+', i)
        if len(url) != 0:
            url = url[0].split(' ')[1]
            req_time = float(i.split(' ')[-1])
            if not temp.get(url):
                temp[url] = [req_time]
                temp['total_count'] += 1
                temp['total_request_time'] += req_time
            else:
                temp[url].append(float(i.split(' ')[-1]))
                temp['total_count'] += 1
                temp['total_request_time'] += req_time
        else:
            errors += 1
    err_perc = errors / total_entries * 100
    logger.info(f'Ошибок при парсинге {errors}, всего записей {total_entries}.')
    return temp, err_perc


def genf(file_content):
    file_content = file_content.decode(encoding='utf-8')
    for i in file_content.split('\n'):
        yield i


# считаем статистику
def counter(d: dict, limit):
    result = []
    for k, v in d.items():
        if k not in ['total_count', 'total_request_time']:
            result.append({
                "url": k,
                "count": len(v),
                "count_perc": round(len(v) / d['total_count'] * 100, 3),
                "time_sum": sum(v),
                "time_perc": round(sum(v) / d['total_request_time'] * 100, 3),
                "time_avg": sum(v) / len(v),
                "time_max": max(v),
                "time_med": median(v),
            })
    result = sorted(result, key=lambda x: x['time_sum'], reverse=True)
    return result[:limit]


# функция создатель отчета
def render_report(stats, path, file_date):
    try:
        with open('report.html', 'r') as f:
            text = f.read()

        text = text.replace('$table_json', str(stats))
        rep_name = os.path.join(path, f'report-{file_date.strftime("%Y.%m.%d")}.html')
        with open(rep_name, 'w') as f:
            f.write(text)
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        logger.error(f'{e}')
        raise


def checkin_dir(directory):
    if not os.path.exists(directory):
        raise NotADirectoryError(
            f"Нужна директория: mkdir {directory}")


def run(config):
    try:
        checkin_dir(config['LOG_DIR'])
        checkin_dir(config['REPORT_DIR'])
        logger.info('start work with logs')
        last_logfile = get_last_logfile(config['LOG_DIR'])
        if last_logfile is not None:
            logger.info(f'working with {last_logfile.name}')
            path = os.path.join(last_logfile.path, last_logfile.name)
            read_cmd = "gzip.open(path, 'rb')" if last_logfile.is_gz else "open(path, 'rb')"  # тернарный оператор
            logger.info(f'reading logfile')
            with eval(read_cmd) as f:
                lfile = f.read()
            generator = genf(lfile)
            logger.info(f'parsing logfile')
            parse_data, err_perc = parser(generator)
            logger.info(f'Не удалось распарсить {round(err_perc, 2)}% логов')
            if err_perc > 30:
                logger.error(f'Превышен 30% допустимых ошибок при парсинге.')
            logger.info(f'counting stats')
            stats = counter(parse_data, config['REPORT_SIZE'])
            logger.info(f'rendering report')
            render_report(stats, config['REPORT_DIR'], last_logfile.date)
        else:
            logger.error('no log files')
        logger.info('done')
    except (Exception, KeyboardInterrupt) as e:
        logger.exception(f'{e}')
        raise


def main(config):
    logger.info('start script')

    parser = argparse.ArgumentParser()

    parser.add_argument('--config',
                        default=config,
                        help="set configuration file path")

    args = parser.parse_args()
    if args.config is not config:
        try:
            logger.info(f'using {args.config} as cfg file')
            with open(args.config, 'r') as f:
                config = json.loads(f.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
            logger.error(f'{e}')
            raise
        except Exception as e:
            logger.exception(f'{e}')
            raise
    else:
        logger.info('using default cfg')
    run(config)


if __name__ == "__main__":
    main(config)
