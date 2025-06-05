import schedule
import time
from datetime import datetime, timedelta
import sys
import subprocess


def job_analyse():
    try:
        file = '__run_analyse__.py'
        print(f'\nAnalyse DB are started at {datetime.now()}\n')
        subprocess.run([sys.executable, file], check=True)
        print(f'\nNext DB checking plan to {datetime.now() + timedelta(minutes=15)}..\n')
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения скрипта: {e}")


def job_excel_check():
    try:
        file = '__run_redis__.py'
        print(f'\nRedis checking are started at {datetime.now()}\n')
        subprocess.run([sys.executable, file], check=True)
        print(f'\nNext redis checking plan to {datetime.now() + timedelta(minutes=10)}..\n')
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения скрипта: {e}")


# Планирование заданий
schedule.every(30).minutes.do(job_analyse)  # Каждые 30 минут
schedule.every(10).minutes.do(job_excel_check)  # Каждые 10 минут


def run():
    try:
        print(f'\nProgramms are starting at {datetime.now()}')
        print(f'\nChecking programm by job_analyse plan to run at {datetime.now() + timedelta(minutes=30)}..\n')
        print(f'\nChecking programm by redis_check plan to run at {datetime.now() + timedelta(minutes=10)}..\n')
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nProgramm were stopped at {datetime.now()}\n")
        sys.exit(0)


if __name__ == '__main__':
    run()