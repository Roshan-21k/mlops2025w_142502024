def average(numbers):
    return sum(numbers)/len(numbers)

def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def to_int(value):
    try:
        return int(value)
    except ValueError:
        return None