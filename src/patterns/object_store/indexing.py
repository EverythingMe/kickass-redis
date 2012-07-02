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
from __future__ import absolute_import

import string

import codecs
import re
from ...util import Rediston, InstanceCache
from .condition import Condition
import pyhash
import logging


class AbstractKey(object):

    def __init__(self, prefix,fields):

        self.prefix = prefix
        self.fields = fields
        self.fieldSet = set(fields)


    def update(self, obj, pipeline = None):
        pass

    def updateMany(self, idsAndKeyValues = ()):
        pass

    def __repr__(self):

        return '%s(%s:%s)' % (self.__class__.__name__, self.prefix, ','.join(self.fields))


class FullTextKey(AbstractKey, Rediston):
    '''
    classdocs
    '''
    
    trantab = string.maketrans("-_'", "   ")
    stopchars = "\"'\\`'[]{}(),./?:)(*&^%$#@!="

    def __init__(self, prefix, alias, fields, objectScoringCallback = None, delimiter = ' '):
        '''
        Constructor
        '''

        AbstractKey.__init__(self, prefix, fields = [alias,])
        Rediston.__init__(self)
        self.fieldSpec = fields #we don't use the key's "fields" to be able to query multiple fields at once
        self.delimiter = delimiter
        self.scoringCallback = objectScoringCallback

    def getKey(self, word):
        
        return 'ft:%s:%s' % (self.prefix, word)
    
        
    def normalizeString(self, str_):
        
        str_ = codecs.encode(str_, 'utf-8')
        
        return str_.translate(self.trantab, self.stopchars)
        
        
    def update(self, obj, pipeline = None):

        score = 1.0

        #if the object suppports scoring, call the callback now
        if self.scoringCallback:
            score = self.scoringCallback(obj)

        pipe = pipeline or self._getPipeline(transaction=False)
        indexKeys = {}
        #split the words
        for field, factor in self.fieldSpec.iteritems():

            for token in re.split(self.delimiter, getattr(obj, field, '')):
                
                t = self.normalizeString(token.lower().strip())
                
                if t:
                    indexKeys[t]= indexKeys.get(t, 0) + float(factor)*score

        for x in indexKeys:

            pipe.zadd(self.getKey(x), obj.id, indexKeys[x])

        if not pipeline:
            pipe.execute()
            
        
    def find(self, condition):


        string = condition.getValuesFor(self.fields[0])[0]

        tokens = filter(None, (self.normalizeString(value.lower().strip()) for value in re.split(self.delimiter, string)))
        
        
        if not tokens:
            return []
        keys = [self.getKey(t) for t in tokens]
        
        destKey = ('tk:%s' % '|'.join(tokens))
        pipe = self._getPipeline(transaction=False)
        pipe.zinterstore(destKey, keys, 'SUM')
        pipe.zrevrange(destKey, 0, -1, False)
        rx = pipe.execute()
        return rx[1]




class UnorderedKey(AbstractKey, Rediston):
    """
    A simple catch all key, non unique, ideal for short texts (emails, etc). case sensitive.
    it uses hashing of the value as a score in a sorted set
    """
    def __init__(self, prefix, fields):
        '''
        Constructor
        '''
        AbstractKey.__init__(self, prefix, fields)
        Rediston.__init__(self)
        self.hasher = pyhash.fnv1a_64()


    def getValue(self, _dict):


        vals = '::'.join(('%s' % _dict[f] for f in self.fields))

        #make a hash val that is 52 bits and can fit as a sorted set score
        hashval = self.hasher(vals) & 0b11111111111111111111111111111111111111111111111111111

        logging.info("Vals for key %s: %s. hashval: %s", self, vals, hashval)

        return hashval

    @InstanceCache
    def redisKey(self):

        return 'k:%s:%s' % (self.prefix, ','.join(self.fields))

    def update(self, obj, pipeline = None):
        """
        Update the key with the value of this object
        """

        hashval = self.getValue(obj.__dict__)
        conn = pipeline or self._getConnection('master')
        conn.zadd(self.redisKey(), **{str(obj.id): hashval})



    def updateMany(self, ids, cls):

        #first, get the values for these ids
        objs = cls.loadObjects(ids, *self.fields)

        #build a dictionary of all the values to be updated
        updateDict = {}
        for obj in objs:
            if obj:
                updateDict[obj.id] = self.getValue(obj.__dict__)

        conn = self._getConnection('master')
        conn.zadd(self.redisKey(), **updateDict)


    def find(self, condition):
        """
        find objects matching  a certian condition
        currently the condition has to be exactly field=value. no multiple options and no ranges allowed
        """
        hashval = self.getValue(condition.fieldsAndValues)
        conn = self._getConnection()
        return conn.zrangebyscore(self.redisKey(), min=hashval,max=hashval,
                                    start = 0 if not condition.paging else condition.paging[0],
                                    num = -1 if not condition.paging else condition.paging[0] + condition.paging[1] )



class OrderedNumericalKey(AbstractKey, Rediston):
    """
    A key for numerical fields (ints or floats) that can sort and page results
    """
    def __init__(self, prefix, field):
        '''
        Constructor
        '''
        AbstractKey.__init__(self, prefix, (field,))
        Rediston.__init__(self)
        self.field = field


    def getValue(self, _dict):

        return float(_dict[self.field])

    @InstanceCache
    def redisKey(self):

        return 'ok:%s:%s' % (self.prefix, ','.join(self.fields))

    def update(self, obj, pipeline = None):
        """
        Update the key with the value of this object
        """
        val = self.getValue(obj.__dict__)
        conn = pipeline or self._getConnection('master')
        conn.zadd(self.redisKey(), **{str(obj.id): val})



    def updateMany(self, ids, cls):

        #first, get the values for these ids
        objs = cls.loadObjects(ids, self.field)

        #build a dictionary of all the values to be updated
        updateDict = {}
        for obj in objs:
            if obj:
                updateDict[obj.id] = self.getValue(obj.__dict__)

        conn = self._getConnection('master')
        conn.zadd(self.redisKey(), **updateDict)


    def find(self, condition):
        """
        find objects matching  a certian condition
        currently the condition has to be exactly field=value. no multiple options and no ranges allowed
        """

        _min = None
        _max = None
        conditionValue = condition.getValuesFor(self.field)[0]
        #if this is a range query, get the min/max of the query
        if isinstance(conditionValue, Condition.Between):
            _min = float(conditionValue.min)
            _max = float(conditionValue.max)
        elif isinstance(conditionValue, Condition.ConditionType):
            _min = _max = float(conditionValue.value)
        else:
            _min = _max = float(conditionValue)

        conn = self._getConnection()
        return conn.zrangebyscore(self.redisKey(), min=_min,max=_max,
            start = 0 if not condition.paging else condition.paging[0],
            num = -1 if not condition.paging else condition.paging[0] + condition.paging[1] )



class UniqueKeyDuplicateError(Exception):

    pass



class UniqueKey(AbstractKey, Rediston):
    """
    Unique value key, for strings or numbers
    It uses a HASH of value=>objectId to make sure no two objects have the same value
    it raises UniqueKeyDuplicateError if you hit a duplicate value
    """
    def __init__(self, prefix, fields):
        '''
        Constructor
        '''
        AbstractKey.__init__(self, prefix, fields)
        Rediston.__init__(self)


    def getValue(self, _dict):


        return '::'.join(('%s' % _dict[f] for f in self.fields))

    @InstanceCache
    def redisKey(self):

        return 'uk:%s:%s' % (self.prefix, ','.join(self.fields))


    def update(self, obj, pipeline = None):
        """
        Update the key with the value of this object
        """

        val = self.getValue(obj.__dict__)
        pipe = self._getPipeline('master')

        #we set and check the value at once, in case the key is already taken we need to check the current object...
        pipe.hsetnx(self.redisKey(), val, obj.id)
        pipe.hget(self.redisKey(), val)

        rx = pipe.execute()
        #this means
        if rx[0] == 0:
            currentObjId= rx[1]
            logging.info("Possible unique key collision, checking... object: %s, current in key: %s", obj.id, currentObjId)

            if currentObjId != '%s' % obj.id:
                logging.warn("Unique Key collision. wanted to set %s but key already has %s", obj, currentObjId)
                raise UniqueKeyDuplicateError("Duplicate error for key %s" % self)
            else:
                logging.info("Unique key set to the same object")
        else:
            logging.info("Unique key %s set new value for %s:%s", self, obj.id, val)

    def updateMany(self, ids, cls):

        #first, get the values for these ids
        objs = cls.loadObjects(ids, *self.fields)

        #build a dictionary of all the values to be updated
        updateDict = {}
        for obj in objs:
            if obj:
                self.update(obj)




    def find(self, condition):
        """
        find objects matching  a certian condition
        currently the condition has to be exactly field=value. no multiple options and no ranges allowed
        """
        val = self.getValue(condition.fieldsAndValues)
        conn = self._getConnection()
        id = conn.hget(self.redisKey(), val)
        return [id] if id is not None else []

