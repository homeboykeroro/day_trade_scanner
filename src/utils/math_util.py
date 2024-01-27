import math

def round_to_nth_digit(value, nth: int = 1) -> int:
    truncate_value = int(value)
    
    if not value or truncate_value == 0:
        return value
    
    digits = int(math.log10(value)) + 1
    divisor = 10 ** (digits - nth)
    
    return round(math.ceil(round(value) / divisor) * divisor)