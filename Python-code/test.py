import sys
import time

print('Sys:  ', sys.platform)


def hello():
    print('hello world')

for i in range(0,2):
    print('upper')
    time.sleep(1)
    for i in range(0,2):
        print('deepest')
        hello()
        time.sleep(2)
