#!/usr/bin/python
"""
Example of using the unique id counter object
"""
__author__ = 'dvirsky'
from util import TimeSampler
from patterns.bitmap_counter import BitmapCounter, IdMapper
import random
import time

import logging

if __name__ == '__main__':


    #logging.basicConfig(level=0)

    #create a non sequential to sequential id mapper if needed. for this example we don't really need it, it's here just to show it
    idMapper = IdMapper('uzrs')

    counter = BitmapCounter('unique_users', timeResolutions=(1,),idMapper=idMapper )

    week = tuple((int(time.time() - i*86400) for i in  xrange(7, 0, -1)))
    print counter.cohortAnalysis(week, 86400)
    #sampling current user
    counter.add(3)


    #Filling with junk entries
    for i in xrange(5000):
        userId = random.randint(1, 1000)
        counter.add(userId)
        time.sleep(0.001)

    timePoints = tuple((time.time() - i for i in  xrange(5, 0, -1)))
    print counter.cohortAnalysis(timePoints, 1)
    #Getting the unique user count for today
#    counter.getCount((time.time(),), counter.RES_DAY)
#
#    #Getting the the weekly unique users in the past minute
#    timePoints = tuple((time.time() - i for i in  xrange(60, 0, -1)))
#    uniq = counter.aggregateCounts(timePoints, counter.OP_TOTAL, timeResolution=1)
#    print "Unique users for range: %s" % uniq
#
#    #average DAU for the past minute
#    avg = counter.aggregateCounts(timePoints, counter.OP_AVG, timeResolution=1)
#    print "Average users per second in range: %s"  % avg

