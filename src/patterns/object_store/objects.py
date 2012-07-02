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
__author__ = 'dvirsky'

import logging
from ...util import  InstanceCache, Rediston
from ..idgenerator import IncrementalIdGenerator


class KeySpec(object):
    """
    Object key descriptor
    """

    def __init__(self, *keys):
        """
        @param keys all the keys to be included in this spec
        """
        self._keys = list(keys)


    def getKey(self, condition):
        """
        Get the proper key for a query
        TODO: Support multiple keys for a single query to be crossed
        @param condition a condition object
        """
        queryKeys = set(condition.fieldsAndValues.iterkeys())
        logging.info("Query keys: %s", queryKeys)

        for key in self._keys:

            if queryKeys == key.fieldSet:
                logging.info("Found key for condition: %s", key)
                return key

        raise ValueError("Could not find key for condition %s", condition)


    def keys(self):
        return self._keys

    @InstanceCache
    def findKeysForUpdate(self, fields):

        fields = set(fields)
        return [key for key  in self._keys if key.fieldSet.intersection(fields)]




class IndexedObject(Rediston):


    #This is the specification of which fields you want to index. override in child classes
    _keySpec = KeySpec()

    #this is the specification of which fields should be saved to redis
    _spec = ('id',)

    #the id generator for the class. by default it is initialized to an incremental id generator
    #you can replace it with another id generator if you want
    _idGenerator = None


    def __init__(self, **kwargs):
        """
        default constructor, override in subclasses to force strict field typing
        """
        self.id = kwargs.get('id', None)
        self.__dict__.update(kwargs)

        #create id generator the first time needed. this can be overriden in child classes
        if not self.__class__._idGenerator:
            self.__class__._idGenerator = IncrementalIdGenerator(self.__name())


    @classmethod
    def createNew(cls, *args, **kwargs):
        """
        Create a new object and save it immeditely
        """
        obj = cls(*args, **kwargs)
        obj.save()
        return obj

    @classmethod
    def __name(cls):
        """
        the object's name for saving
        """
        if not hasattr(cls, '_oredis_name'):
            setattr(cls, '_oredis_name', cls.__name__.lower())

        return cls._oredis_name

    @classmethod
    def __createId(cls):
        """
       create a new id for an object
        """

        return cls._idGenerator.getId()


    @classmethod
    def __key(cls, id):
        """
        Get the actual id of an object based on a given id
        """
        return '%s:%s' % (cls.__name(), id)

    @classmethod
    def config(cls, host, port, db, timeout = None):

        for k in cls._keySpec.keys():
            k.__class__.config(host, port, db, timeout)

        cls._host = host
        cls._port = port
        cls._db = db
        cls._timeout = timeout


        cls.__connPool = None

    @classmethod
    def loadObjects(cls, ids, *fields):
        """
        Load a list of objects by ids
        @param ids a list of object ids (not keys)
        @param fields optional list of fields to pass if you do not want ALL the object
        """

        p = cls._getPipeline()

        if not fields:

            [p.hgetall(cls.__key(id)) for id in ids]
        else:
            #we get the id anyway,no point in getting it from redis
            if 'id' in fields:
                fields.remove('id')
            [p.hmget(cls.__key(id), fields) for id in ids]

        ret = p.execute()

        objs = []
        for idx,r in enumerate(ret):
            if r:
                r['id'] = ids[idx]
                obj = cls( **r)
                objs.append(obj)
        return objs


    def __repr__(self):

        return '%s(%s)' % (self.__class__.__name__, self.__dict__)

    @classmethod
    def getAll(cls, first = 0, num = -1, *fields):
        """
        Get all the objects of a given type, with optional paging
        """
        redisConn = self._getConnection()
        ids = redisConn.zrange(cls.__classKey(), first, num)
        return cls.loadObjects(ids, redisConn, *fields)




    def __index(self, pipeline = None):
        """
        update the object's indexes
        """

        if not self._keySpec:
            return

        p = pipeline or self._getPipeline('master', transaction=False)
        for k in self._keySpec.keys():

            k.update(self, p)
        if not pipeline:
            p.execute()

    @classmethod
    def __classKey(cls):
        """
        Get the master key of all the available ids in the class
        """
        return 'ids:%s' % cls.__name()

    def __getId(self):
        """
        Get the object's id or create a new one if needed
        """


        if self.id is None:
            self.id = self.__createId()
        return self.id

    def save(self):

        #redisConn = self._getConnection('master')
        _id = self.__getId()

        saveDict = {k: getattr(self, k, None) for k in self._spec}

        pipe =self._getPipeline('master', True)
        #save all properties
        pipe.hmset(self.__key(_id), saveDict )
        #add the id to the master object list
        pipe.zadd(self.__classKey(), **{str(_id): float(_id)})

        #index all the relevant keys
        self.__index(pipe)
        pipe.execute()




    def update(self, **keyValues):
        """
        Set a field(s) in the object and save it to the database
        @param keyValues free form x=y kwargs
        """

        if not self.id:
            raise ValueError("Cannot update a value for an unsaved object")

        #set the data in redis
        ret = self._getConnection('master').hmset(self.__key(self.id), keyValues )

        #update the keys
        updateAbleKeys = self._keySpec.findKeysForUpdate(keyValues.iterkeys())
        for k in updateAbleKeys:
            k.update(self, keyValues)

        #set the data in the object (after successful redis update, to avoid invalid objects)
        for k, v in keyValues.iteritems():
            setattr(self, k, v)

        return ret


    @classmethod
    def incrementWhere(cls, condition, fieldName, amount):
        """
        Increment a field by an amount, updating the database
        @return a list of updated ids and the new value afterupdate of the field
        """

        ids = cls.find(condition)
        pipe = cls._getPipeline('master')
        for id in ids:
            pipe.hincrby(cls.__key(id), fieldName, amount)

        #execute the pipe and get the new values
        newVals = pipe.execute()

        #udpate the keys
        updateAbleKeys = cls._keySpec.findKeysForUpdate((fieldName,))
        for key in updateAbleKeys:
            key.updateMany(((id, newVals[idx]) for idx, id in enumerate(ids)))


        return [(id, newVals[idx]) for idx, id in enumerate(ids)]

    @classmethod
    def updateWhere(condition, **keyValues):
        """
        Update fields with new values for objects matching a condition
        """
        ids = cls.find(condition)
        pipe = self._getPipeline('master')
        #update the database
        for id in ids:
            pipe.hmset(cls.__key(id), keyValues)

        #execute the pipe and get the new values
        res = pipe.execute()

        #udpate the keys
        updateAbleKeys = self.__keySpec.findKeysForUpdate(keyValues.iterkeys())
        for key in updateAbleKeys:
            key.updateMany(((id, keyValues) for ids in ids))

        return ids

    @classmethod
    def get(cls, condition, *fields):
        """
        Load a class by a named key indexing some if its fields
        value can by a multiple token string
        """

        ids = cls.find(condition)

        return cls.loadObjects(ids, *fields )

    @classmethod
    def find(cls, condition):
        """
        Find object ids for a given condition
        """

        key = cls._keySpec.getKey(condition)

        ids = key.find(condition)

        logging.debug("Ids for %s: %s", condition, ids)
        return ids