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


import redis
import sys
import contextlib
import re

class RedisAssertionError(Exception):

    def __init__(self, msg, *args):

        if args:
            msg = msg % args
        Exception.__init__(self, msg)




class RedisDataTest(object):
    """
    This class has a unittest like API that enables testing redis databases for consistency and automatic assertions about data
    It can either be used by calling these functions, or by inheriting it, implementing tests in fucntions starting with \
    "test", and then calling run() on the test instance.
    Assertion failures raise the RedisAssertionError exception
    """

    T_STRING = 'string'
    T_HASH = 'hash'
    T_ZSET = 'zset'
    T_LIST = 'list'
    T_SET = 'set'

    ############################################################################################
    ## Static methods below are used to validate values of keys by applying lambdas on them
    # Example: test.assertKeyValue('foo', RedisDataTest.greaterThan(0)) => asserts that the value in 'foo' > 0
    @staticmethod
    def equals(y):
        return lambda x: x == y
    @staticmethod
    def greaterThan(y):
        return lambda x: float(x) > y

    @staticmethod
    def greaterThanOrEqual(y):
        return lambda x: x >= y

    @staticmethod
    def lessThan(y):
        return lambda x: x < y

    @staticmethod
    def lessThanOrEqual(y):
        return lambda x: x <= y
    @staticmethod
    def matches(exp):
        return lambda x: bool(re.match(exp, x))

    @staticmethod
    def isNumeric(x):
        try:
            float(x)
            return True
        except ValueError:
            return False

    #End of validation lambdas
    #########################################


    def __init__(self, host = 'localhost', port = 6379, db = 0, timeout = None, verbose = True):
        """
        @param verbose whether we want to output messages or not
        """
        self.redis = redis.Redis(host, port, db, timeout)
        self.verbose = verbose

    @contextlib.contextmanager
    def _message(self, msg, *args):
        """
        Wrapping tests with messages and [PASS] stamps
        """
        if self.verbose:
            if args:
                msg = msg % args
            sys.stderr.write(msg + '...')
        yield

        if self.verbose:
            sys.stderr.write('\t[PASS]\n')


    def assertKeysExists(self, *keys):
        """
        Assert that a key(s) exists in redis
        """

        with self._message("Asserting the existence of %s (%d keys)...", keys[:5], len(keys)):
            p = self.redis.pipeline()
            [p.exists(k) for k in keys]
            for idx, exists in enumerate(p.execute()):
                if not exists:
                    raise RedisAssertionError("Key %s does not exist" % keys[idx])


    def assertKeysType(self,  ktype, *keys):
        """
        Assert that a bunch of keys all belong to a type
        @param ktype a string representing the type, use the T_* static constants, e.g RedisDataTest.T_STRING
        """

        with self._message("Testing if '%s (%d keys)' is of type %s", keys[:5], len(keys), ktype):
            p = self.redis.pipeline()
            [p.type(k) for k in keys]
            for idx, t in enumerate(p.execute()):
                if not t==ktype:
                    raise RedisAssertionError("Key %s is of type %s - expected %s" % (keys[idx], t, ktype))

    def countPrefix(self, prefix):
        """
        Return the number of keys in a prefix
        @warning: DO NOT USE IN PRODUCTION DATABASES!
        """
        keys = self.redis.keys(prefix)
        return len(keys)


    def assertPrefixCount(self, prefix, minAmount, maxAmount = None):
        """
        Check that a prefix does not appear more than maxAmount times and no less of minAmount times
        @param prefix - the prefix to be tested
        @warning: DO NOT USE IN PRODUCTION DATABASES!
        """
        with self._message("Assering that keys with prefix '%s' are between %s and %s", prefix, minAmount, maxAmount or 'infinity'):
            num = self.countPrefix(prefix)
            if num < minAmount:
                raise RedisAssertionError("Expected at least %d elements for '%s', got %d", minAmount, prefix, num)
            elif maxAmount is not None and num > maxAmount:
                raise RedisAssertionError("Expected at most %d elements for '%s', got %d", maxAmount, prefix, num)


    def assertListSize(self, key, min, max = None):
        """
        Assert the size of a LIST is between min and max. if max is none only min applies
        """
        self.__assertLen(key, 'llen', min, max = None)

    def assertHashLen(self, key, min, max = None):
        """
        Assert the size of a HASH is between min and max. if max is none only min applies
        """
        self.__assertLen(key, 'hlen', min, max = None)

    def assertSetCardinality(self, key, min,max = None):
        """
        Assert the size of a set is between min and max. if max is none only min applies
        """
        self.__assertLen(key, 'scard', min, max)

    def assertSortedSetSize(self, key, min, max = None):
        """
        Assert the size of a sorted set is between min and max. if max is none only min applies
        """
        self.__assertLen(key, 'zcard', min, max)

    def assertStringLength(self, key, min, max = None):

        self.__assertLen(key, 'strlen', min, max = None)

    def __assertLen(self, key, command, min, max):
        """
        Internal function. Assert the size of a an element is between min and max. if max is none only min applies
        """
        with self._message('Checking length of %s, between %s and %s', key, min, max or 'infinity'):
            num = self.redis.execute_command(command, key)
            if num < min:
                raise RedisAssertionError("Expected at least %d elements for '%s', got %d", min, key, num)
            elif max is not None and num > max:
                raise RedisAssertionError("Expected at most %d elements for '%s', got %d", min, key, num)


    def assertHashKeysExist(self, key, *fields):
        """
        Assert that several sub-keys exist in a hash
        """
        with self._message("Checking if keys %s exist in hash %s", fields, key):

            hkeys = set(self.redis.hkeys(key))
            nonExistentKeys = [k for k in fields if k not in hkeys]
            if nonExistentKeys:
                raise RedisAssertionError("Keys '%s' of hash %s do not exist!", nonExistentKeys, key)

    def assertKeyValue(self, key, expectedValueOrCallback):
        """
        Make an assertion about the value of a certain key.
        @param expectedValueOrCallback if set to a non callable object - we just compare it. else - we run the callback
        use the static lambda returning callbacks in the beginning of this module here
        """
        with self._message("Checking value for %s", key):
            val = self.redis.get(key)

            if callable(expectedValueOrCallback):
                rc = expectedValueOrCallback(val)
            else:

                rc = val == expectedValueOrCallback
            if not rc:
                raise RedisAssertionError("Value test failed on %s. got value %s", key,  val)

    def assertHashValue(self, key, hkey, valueTestCAllback = lambda x: x is not None, testDescription = None):
        """
        Make an assertion about the value inside a sub-key of a HASH
        @param key the HASH
        @param hkey the HASH sub key
        @param valueTestCAllback a function that makes an assertion about the value. by default we check that it's not null
        """
        with self._message("Checking value for %s in hash %s", hkey, key):
            val = self.redis.hget(key, hkey)
            if not valueTestCAllback(val):
                raise RedisAssertionError("Hash Value test %s failed on %s/%s. got value %s", testDescription or '', key, hkey, val)


    def assertValueInSortedSet(self, key, value, assertRank = None, revrank = False):
        """
        Check if a value is inside a sorted set
        @param key the sorted set's key
        @param value the value inside the sorted set
        @param assertRank if set to a number, we check that the rank of the value is identical to this
        @param revrank if set to True, we use ZREVRANK to assert the rank
        """
        with self._message("Checking value %s in sorted set %s", value, key):


            rank = (self.redis.zrank if not revrank else self.redis.zrevrank)(key, value)
            if rank is None:
                raise RedisAssertionError("value %s is not in sorted set %s", value, key)

            if assertRank is not None and assertRank != rank:
                raise RedisAssertionError("value %s in sorted set %s has rank of %s, expected %s", value, key, rank, assertRank)


    def run(self):

        """
        Run all functions that start with test*
        """

        for memberName, member in self.__class__.__dict__.iteritems():

            if memberName.startswith('test') and callable(member):

                getattr(self, memberName)()


if __name__ == '__main__':

    class MyTest(RedisDataTest):

        def testMe(self):

            self.assertKeysExists('foo')


    t = MyTest()
    t.run()