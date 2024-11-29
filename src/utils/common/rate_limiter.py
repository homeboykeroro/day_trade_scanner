import threading
import time

class RateLimiter:
    def __init__(self, rate: int, per: int):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        with self.lock:
            current = time.time()
            time_passed = current - self.last_check
            self.last_check = current
            self.allowance += time_passed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate

            print(f"{threading.current_thread().name}, time passed: {time_passed:.4f} seconds, allowance before acquiring: {self.allowance:.2f}")
            if self.allowance < 1.0:
                time_to_sleep = (1.0 - self.allowance) * (self.per / self.rate)
                print(f"{threading.current_thread().name}, sleeping for {time_to_sleep:.4f} seconds")
                time.sleep(time_to_sleep)
                self.allowance = 0
            else:
                self.allowance -= 1.0
            print(f"{threading.current_thread().name}, allowance after acquiring: {self.allowance:.2f}")
