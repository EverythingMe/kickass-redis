#Copyright 2012 Do@. All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are
#permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice, this list of
#      conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright notice, this list
#      of conditions and the following disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY Do@ ``AS IS'' AND ANY EXPRESS OR IMPLIED
#WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#The views and conclusions contained in the software and documentation are those of the
#authors and should not be interpreted as representing official policies, either expressed
#or implied, of Do@.

__author__ = 'dvirsky'

from util import Rediston, TimeSampler, InstanceCache
import logging
import time

from idgenerator import  IncrementalIdGenerator

class BitmapCounter(Rediston):
    """
    This class wraps a unique id counter, mainly to be used as a user counter
    It can be set to sample specific time resolutions, the default is daily
    It can aggregate several time resolutions into one count, to be cached for memory optimization
    (TBD: the calss should be able to do this)
    """

    RES_DAY = 86400
    RES_HOUR = 3600
    RES_MINUTE = 60

    OP_TOTAL = 'TOTAL'
    OP_AVG = 'AVG'
    OP_INTERESECT = 'INTERSECT'



    def __init__(self, metricName, timeResolutions = (86400,), idMapper = None):
        """
        Constructor
        @param metricName the name of the metric we're sampling, to be used as the redis key
        @param timeResolutions a tuple of resolutions to be sampled, in seconds
        @param idMapper optional IdMapper object that can convert non sequential ids to sequential ones
        NOTE: there will be an extra counter key in redis for each resolution, so lots of resolutions can cause huge RAM overhead
        """
        self.metric = metricName
        self.timeResolutions = timeResolutions
        self.idMapper = idMapper

    def __getKey(self, timestamp, resolution):
        """
        Get the redis key for this object, for internal use
        """

        return 'uc:%s:%s:%s' % (self.metric, resolution, int(timestamp - (timestamp % resolution)))


    def add(self, objectId, timestamp = None, sequentialIdMappingPrefix = None):
        """
        Add one sample.
        TODO: enable multiple samples in one pipeline
        @param objectId an integer of the object's id. NOTE: do not use this on huge numbers
        @param timestamp the event time, defaults to now
        """

        #map to a sequential id if needed
        if self.idMapper:
            objectId = self.idMapper.getSequentialId(objectId)

        timestamp = timestamp or time.time()

        #get the keys to sample to
        keys = [self.__getKey(timestamp, res) for res in self.timeResolutions]
        pipe = self._getPipeline()
        #set the bits
        [pipe.setbit(key, int(objectId), 1) for key in keys]
        pipe.execute()

    def isSet(self, objectId, timestamp, timeResolution = None):
        """
        Tell us whether a specific objectId is set in the counter for a specific resolution
        @param objectId the object to test
        @param timestamp the time to test
        @param timeResolution the time slot to test, defaults to the first resolution given to the counter
        """
        timeResolution = timeResolution or self.timeResolutions[0]
        key = self.__getKey(timestamp, timeResolution)
        return self._getConnection().getbit(key, objectId)


    def getCount(self, timestamps, timeResolution = None):
        """
        Count the cardinality of time slots
        @param timestamps a list of timestamps to test
        @param timeResolution the time slot to aggregate, defaults to the first resolution given to the counter
        @return a list of [(timestamp, count), ...]
        """
        timeResolution = timeResolution or self.timeResolutions[0]
        pipe = self._getPipeline()

        [pipe.execute_command('BITCOUNT', self.__getKey(timestamp, timeResolution)) for timestamp in timestamps]
        return zip(timestamps, pipe.execute())

    def aggregateCounts(self,  timestamps, op = OP_TOTAL,timeResolution = None ):
        """
        Aggregate a few time slots, either summing the unique total, average or memebers in all slots
        @param timestamps a list of timestamps to test
        @param op should be one of SUM, AVG, INTERSECT
        @param timeResolution the time slot to aggregate, defaults to the first resolution given to the counter
        """
        timeResolution = timeResolution or self.timeResolutions[0]

        if op not in  (BitmapCounter.OP_INTERESECT, BitmapCounter.OP_TOTAL, BitmapCounter.OP_AVG):
            raise ValueError("Invalid aggregation op %s" % op)

        if op == BitmapCounter.OP_INTERESECT:
            bitop = 'AND'
        else:
            bitop = 'OR'

        dest = 'aggregate:%s:%s' % (self.metric,hash(timestamps))
        pipe = self._getPipeline()
        pipe.execute_command('BITOP',bitop, dest, *(self.__getKey(timestamp, timeResolution) for timestamp in timestamps))
        pipe.execute_command('BITCOUNT', dest)
        rx = pipe.execute()
        ret = rx[1]
        if op ==  BitmapCounter.OP_AVG:
            return float(ret)/len(timestamps)
        else:
            return ret

    def cohortAnalysis(self, timestamps, timeResolution):
        """
        Given a list of timestamps, generates a list of retention measures of the first timestamp, for each later timestamp
        @return a list of tuples [(timestamp,num),...]
        """

        #put the first timestamp as the first record in what we return
        ret = self.getCount((timestamps[0],), timeResolution)
        conn = self._getConnection()
        #get the count for each timestamp
        for idx, ts in enumerate(timestamps[1:]):
            dest = 'cohort:%s:%s:%s' % (self.metric,ts,idx)
            conn.execute_command('BITOP', 'AND', dest, self.__getKey(timestamps[0], timeResolution),
                                  self.__getKey(ts, timeResolution))

            count = conn.execute_command('BITCOUNT', dest)
            print ts, count
            ret.append((ts,count))
            conn.expire(dest, 60)


        return ret


    def funnelAnalysis(self, timestamps, timeResolution):
        """
        Given a list of timestamps, return a funnel analysis - i.e. for each timestamp, an interesection of it and all the previous points
        @return a list of tuples [(timestamp,num),...]
        """
        conn = self._getConnection()

        prev = None
        ret = []
        #get the count for each timestamp
        for i in xrange(len(timestamps)):
            dest = 'funnel:%s:%s:%s' % (self.metric,timestamps[i],i)
            conn.execute_command('BITOP', 'AND', dest, prev or self.__getKey(timestamps[0], timeResolution),
                                 self.__getKey(timestamps[i], timeResolution))

            count = conn.execute_command('BITCOUNT', dest)
            prev = dest
            logging.info("Funnel for timestamp %s: %s", timestamps[i], count)
            ret.append((timestamps[i],count))
            conn.expire(dest, 60)


        return ret



class IdMapper(Rediston):

    """
    This class creates a compact mapping between a non sequential object id to a sequential id
    Create a mapper an pass it to the bitmap counter if you want to convert big or textual ids to sequentials
    """


    def __init__(self, prefix):

        self.prefix = prefix
        self.idgen = IncrementalIdGenerator(namespace=self._redisKey())

    def _redisKey(self):

        return 'idmap:%s' % self.prefix

    def getSequentialId(self, objectId):
        """
        Convert a non sequential id to sequential id, by either creating a new mapping or retrieving an old one
        """
        conn = self._getConnection()
        rc = conn.hget(self._redisKey(), objectId)
        if rc:
            logging.info("Found id mapping for %s:%s: %s", self.prefix, objectId, rc)
            return rc
        else:
            id = self.idgen.getId()
            rc = conn.hsetnx(self._redisKey(), objectId, id)
            if rc: #the write was successful
                logging.info("Created new sequential id for %s:%s: %s", self.prefix, objectId, rc)
                return id
            else: #possible race con
                logging.info("Got new sequential id for %s:%s: %s", self.prefix, objectId, rc)
                return conn.hget(self._redisKey(), objectId)







