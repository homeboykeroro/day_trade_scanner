import math
import decimal

def round_to_nth_digit(value, nth: int = 1) -> int:
    truncate_value = int(value)
    
    if not value or truncate_value == 0:
        return value
    
    digits = int(math.log10(value)) + 1
    divisor = 10 ** (digits - nth)
    
    return round(math.ceil(round(value) / divisor) * divisor)

def get_no_of_decimal_places(num):
    return abs(decimal.Decimal(str(num)).as_tuple().exponent)

def get_max_round_decimal_places(num):
    if num >= 0 and num < 1: 
        max_round_precision = 3
    elif num > 1 and num <= 10:
        max_round_precision = 2
    else:
        max_round_precision = 1
    
    return max_round_precision

def get_first_non_zero_decimal_place_position(num):
    decimal_part = str(num).split('.')[1]

    for i, digit in enumerate(decimal_part):
        if digit != '0':
            return i + 1    

    return 0