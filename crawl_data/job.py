import schedule
import time
from producer import send_message


if __name__ == "__main__":
    while True:
        send_message()
        time.sleep(1) 