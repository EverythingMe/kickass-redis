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

from ..util import Rediston, InstanceCache
from threading import RLock
from collections import deque
import logging


class IncrementalIdGenerator(Rediston):
    """
    This class generates guaranteed unique incremental object ids, using redis, that can be pulled from many machines.
    To optimize performance, it doesn't need to request a new id from redis each time.
    Instead it reserves N ids in the clients, and hands them out until they run out.
    This is thread safe and can be used from multiple processes or machines
    """
    def __init__(self, namespace, maxReserveBuffer = 100):
        """
        @param namespace this is the namespace of the ids to be generated. each namespace is a single id generator
        @param maxReserveBuffer how many ids we want to reserve from redis. If you can't accept any "holes" in object
        ids in case of a process crash, set this to 1
        """
        self.namespace = namespace
        self.maxReserveBuffer = maxReserveBuffer
        self.reservedIdsCache = deque()
        self.__lock = RLock()

    @InstanceCache
    def __redisKey(self):
        """
        The redis key for the generator
        """
        return ':%s:idgen' % self.namespace

    def __reserveIds(self):
        """
        Call redis and reserve more ids
        """
        with self.__lock:
            conn = self._getConnection('master')
            res = conn.incr(self.__redisKey(), self.maxReserveBuffer)
            self.reservedIdsCache = deque((res - (self.maxReserveBuffer - i - 1) for i in xrange(self.maxReserveBuffer)))

            #logging.info("Reserved new ids: %s", list(self.reservedIdsCache))

    def getId(self):
        """
        Pop one id, request more from redis if there aren't any left in the cache
        """
        with self.__lock:
            try:
                return self.reservedIdsCache.popleft()
            except IndexError: #queue is empty
                self.__reserveIds()
                return self.reservedIdsCache.popleft()






