import time
from datetime import datetime

def write_logs_forever(filename):
    with open(filename, 'a') as f:
        while True:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            log_message = f"[{now}]: Acesta este un log de test\n"
            f.write(log_message)
            f.flush() 

if __name__ == "__main__":
    log_file = "./mountPoint/testLog"
    write_logs_forever(log_file)