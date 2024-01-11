import schedule
import time
from producer import send_message

def crawl_and_print():
    print("Job started at 7:00 AM.")
    send_message()
    print("Job completed.")

if __name__ == "__main__":
    schedule.every().day.at("07:00").do(crawl_and_print)
    while True:
        schedule.run_pending()
        time.sleep(1)