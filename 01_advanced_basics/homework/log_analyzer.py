"""This module does blah blah."""
#!/usr/bin/env python
# -*- coding: utf-8 -*-


# log_format ui_short
# '$remote_addr  $remote_user $http_x_real_ip [$time_local] "$request" '
# '$status $body_bytes_sent "$http_referer" '
# '"$http_user_agent" "$http_x_forwarded_for" "$http_X_REQUEST_ID" "$http_X_RB_USER" '
# '$request_time';

import argparse
import json
import logging
import os
import re
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


def check_format(string):
    """проверка формата"""
    return bool(re.findall(r'[\.0-9][g0-9][z0-9]$', string))


def get_last_logfile(mypath):
    """функция которая ищет последний лог удобно возвращать namedtuple"""
    temp = []
    for (dirpath, dirnames, filenames) in os.walk(mypath):
        for i in filenames:
            if '-ui.' in i:
                temp.append(i)
        break
    if temp:
        temp.sort()
        last_log_name = temp[-1]
        if check_format(last_log_name):
            Logfile = namedtuple("Logfile", 'path name date is_gz')
            date = re.findall(r'\d*\.\d+|\d+', last_log_name)
            date_obj = datetime.strptime(date[0], '%Y%m%d')
            last_log_obj = Logfile(mypath, last_log_name, date_obj, '.gz' in last_log_name)
            return last_log_obj
        logger.error('Only support plain text or .gz files')


def parser(gen):
    """# функция парсер лога"""
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
    logger.info('Ошибок при парсинге %s, всего записей %s.', errors, total_entries)
    return temp, err_perc


def genf(file_content):
    """Функция генератор"""
    file_content = file_content.decode(encoding='utf-8')
    for i in file_content.split('\n'):
        yield i


def counter(dictionary: dict, limit):
    """считаем статистику"""
    result = []
    for key, val in dictionary.items():
        if key not in ['total_count', 'total_request_time']:
            result.append({
                "url": key,
                "count": len(val),
                "count_perc": round(len(val) / dictionary['total_count'] * 100, 3),
                "time_sum": sum(val),
                "time_perc": round(sum(val) / dictionary['total_request_time'] * 100, 3),
                "time_avg": sum(val) / len(val),
                "time_max": max(val),
                "time_med": median(val),
            })
    result = sorted(result, key=lambda x: x['time_sum'], reverse=True)
    return result[:limit]


def render_report(stats, rep_name):
    """функция создатель отчета"""
    try:
        with open('report.html', 'r', encoding='utf-8') as file:
            text = file.read()

        text = text.replace('$table_json', str(stats))
        # rep_name = os.path.join(path, f'report-{file_date.strftime("%Y.%m.%d")}.html')
        with open(rep_name, 'w', encoding='utf-8') as file:
            file.write(text)
    except (FileNotFoundError, json.decoder.JSONDecodeError) as error:
        logger.error('%s', error)
        raise


def checkin_dir(directory):
    """проверка что папка существует"""
    if not os.path.exists(directory):
        raise NotADirectoryError(
            f"Нужна директория: mkdir {directory}")


def check_repeat(rep, repdir):
    """функция проверяет что скрипт не отрабатывал с этим лог файлом"""
    temp = []
    for (dirpath, dirnames, filenames) in os.walk(repdir):
        temp = filenames
        break
    return rep in temp


def run(config):
    """Run log analyzer"""
    try:
        checkin_dir(config['LOG_DIR'])
        checkin_dir(config['REPORT_DIR'])
        logger.info('start work with logs')
        last_logfile = get_last_logfile(config['LOG_DIR'])
        if last_logfile is not None:
            rep_name = f'report-{last_logfile.date.strftime("%Y.%m.%d")}.html'
            if check_repeat(rep_name, config['REPORT_DIR']):
                logger.info('report done already')
            else:
                rep_name = os.path.join(config['REPORT_DIR'], rep_name)

                logger.info('working with %s', last_logfile.name)
                path = os.path.join(last_logfile.path, last_logfile.name)
                read_cmd = "gzip.open(path, 'rb')" if last_logfile.is_gz else "open(path, 'rb')"
                logger.info('reading logfile')
                with eval(read_cmd) as file:
                    lfile = file.read()
                generator = genf(lfile)
                logger.info('parsing logfile')
                parse_data, err_perc = parser(generator)
                logger.info('Не удалось распарсить %s процента логов', round(err_perc, 2))
                if err_perc > 30:
                    logger.error('Превышен 30% допустимых ошибок при парсинге.')
                logger.info('counting stats')
                stats = counter(parse_data, config['REPORT_SIZE'])
                logger.info('rendering report')
                render_report(stats, rep_name)
        else:
            logger.error('no log files to analyze')
        logger.info('script done')
    except (Exception, KeyboardInterrupt) as error:
        logger.exception('%s', error)
        raise


def main(config):
    """The main func"""
    logger.info('start script')

    parser = argparse.ArgumentParser()

    parser.add_argument('--config',
                        default=config,
                        help="set configuration file path")

    args = parser.parse_args()
    if args.config is not config:
        try:
            logger.info('using %s as cfg file', args.config)
            with open(args.config, 'r', encoding='utf-8') as file:
                config = json.loads(file.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError) as error:
            logger.error('%s', error)
            raise
        except Exception as error:
            logger.exception('%s', error)
            raise
    else:
        logger.info('using default cfg')
    run(config)


if __name__ == "__main__":
    main(config)
