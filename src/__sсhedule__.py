import schedule
import time
from datetime import datetime, timedelta
import sys
import subprocess


def job():
    try:
        file = '__run_analyse__.py'
        print(f'\nAnalyse are started at {datetime.now()}\n')
        subprocess.run([sys.executable, file], check=True)
        print(f'\nNext checking plan to {datetime.now() + timedelta(minutes=5)}..\n')
    except subprocess.CalledProcessError as e:
        print(f"Ошибка выполнения скрипта: {e}")


schedule.every(5).minutes.do(job)

def run():
    try:
        while True:
            schedule.run_pending()
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nProgramm was stopped {datetime.now()}\n")
        sys.exit(0)


if __name__== '__main__':
    print(f'\nProgramm is starting at {datetime.now()}')
    print(f'\nChecking programm plan to run at {datetime.now() + timedelta(minutes=5)}..\n')
    run()
