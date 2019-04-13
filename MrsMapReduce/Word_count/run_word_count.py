import os
import sys
import time

filename = sys.argv[1]

print("starting Mrs MapReduce on :", filename)
start = time.time()
os.system('python word_count.py ' + filename + ' outPut')
end = time.time()
totalTime = (end - start)
print("proccess took :",totalTime )



