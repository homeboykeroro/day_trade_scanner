import threading
import time

from utils.logger import Logger

logger = Logger()

class RateLimiter:
    def __init__(self, rate: int, per: int):
        self.rate = rate
        self.per = per
        self.allowance = rate
        self.last_check = time.time()
        self.lock = threading.Lock()

    def acquire(self, method_name: str = ''):
        with self.lock:
            current = time.time()
            time_passed = current - self.last_check
            self.last_check = current
            self.allowance += time_passed * (self.rate / self.per)
            if self.allowance > self.rate:
                self.allowance = self.rate

            logger.log_debug_msg(f"{threading.current_thread().name}, time passed: {time_passed:.4f} seconds, method name: {method_name}, rate: {self.rate}, per: {self.per}, allowance before acquiring: {self.allowance:.2f}", with_std_out=True)
            if self.allowance < 1.0:
                time.sleep(self.per)
                self.allowance = 0
            else:
                self.allowance -= 1.0
            logger.log_debug_msg(f"{threading.current_thread().name}, method name: {method_name}, rate: {self.rate}, per: {self.per}, allowance after acquiring: {self.allowance:.2f}", with_std_out=True)
