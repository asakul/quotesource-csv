'''

'''

def float_to_fixed(value):
    i = int(value)
    return (i, int((value - i) * 1000000000) )