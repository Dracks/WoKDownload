'''
Created on Feb 8, 2012

@author: dracks
'''
import time
import random
#import codecs

def debugWrite(data):
    #tmp=codecs.open("DebugWrite.xml", "w", "utf-8")
    tmp=open("DebugWrite.xml", "w")
    data=data.split("\n")
    count=0
    for row in data:
        count+=1
        #print count
        tmp.write(row)
    tmp.close()

def debug(br, num=''):
    tmp=open('out%s.html' % num, 'w')
    tmp.write(br.response().read())
    tmp.close()

def printWithPosition(l):
    for i in range(0,len(l)):
        print i,": ",l[i]

def randomSleep():
    time.sleep(3+random.gauss(0,0.5))