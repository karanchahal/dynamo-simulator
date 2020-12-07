import asyncio
import time



def hey(n):
    def boo():
        nonlocal n
        print(n)
        n += 1
    return boo

k = hey(1)
# l = make_multiplier_of(10)
# print(l(2))
k()
k()