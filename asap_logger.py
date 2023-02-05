import sys

from datetime import datetime


log_dir = "D:\stock_automation\logs"
last_log_dir = "D:\stock_automation\logs\last_log"

log_path = ""
last_log_path = ""

def get_func_name():
    return sys._getframe(1).f_code.co_name

def logger_init(func_name):
    global log_path, last_log_path

    log_path = log_dir + "\\" + func_name + ".txt"
    last_log_path = last_log_dir + "\\" + func_name + "_last.txt"

    open(last_log_path, 'w', encoding='utf-8')

def write_log(func_name, log):
    global log_path, last_log_path

    time = datetime.now().strftime("%Y-%m-%d %H:%M %S")
    log_msg = f"[{time}][{func_name}]: {log}\n"

    with open(log_path, 'a', encoding='utf-8') as f:
        f.write(log_msg)

    with open(last_log_path, 'a', encoding='utf-8') as f:
        f.write(log_msg)
