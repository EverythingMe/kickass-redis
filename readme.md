Kickass-Redis - a loose framework of Redis based data solutions
==================================================

This project aims to create a repository of useful python libraries built on top of redis (and using each other),
to automate data modeling with Redis.

##For discussion, help and contributing code - join the google group at https://groups.google.com/forum/#!forum/kickass-redis

Redis is relatively low level, and while it is simple to start using, getting a good knowledge of how to model problems
with it in an efficient way can be tricky. So I've created this project to wrap common use cases, into a loose framework
of redis based solutions for real world problems.

The project has started out as a bunch of code examples for a presentation I recently gave on [IL Tech Talks](http://www.iltechtalks.org.il/),  that can be found here: http://www.slideshare.net/dvirsky/kicking-ass-with-redis

##Disclaimer:

This project, while some snippets of it are taken from code I run in production in other projects, is an initial release,
and should not be taken to be stable production code. As everything.me (and hopefully other people) starts using more and more
of this code, it will become more mature and complete. But for now, take it as an alpha release, intended to get people involved.

####Having said that, most of this stuff is pretty basic, and should work without problems, or be easily fixable :)

#Components
----------

To kick things off, the framework includes the following components:

## object_store

a fast yet simple ORM (well, OM actually) that automates creation, indexing and searching for complex objects using redis.

Indexes include: simple string index, numeric index that supports sorting and ranges, simplistic full text index, and a unique key.

###Example:

```python
from patterns.object_store.objects import IndexedObject, KeySpec
from patterns.object_store.indexing import UnorderedKey, OrderedNumericalKey

class User(IndexedObject):


    #which fields should be saved to redis
    _spec = ('id', 'name', 'email', 'pwhash', 'registrationDate', 'score')

    #The keys for this object
    _keySpec = KeySpec(
        UnorderedKey(prefix='users',fields=('name',)),
        OrderedNumericalKey(prefix='users', field='score')
    )

    def __init__(self, **kwargs):
        IndexedObject.__init__(self, **kwargs)
        self.registrationDate = int(kwargs.get('registrationDate', time.time()))

#Creating a user
user = User(email = 'user@domain.com', name = 'John Doe', pwhash = 'eabc626ec26bc6ae6cb2', score = 100)
user.save()

#loading by name key
users =  User.get(Condition({'name': 'John Doe'}))

#loading by id:
users = User.loadObjects((1,))

#See example/users_example for a more detailed exmample and some benchmarks
```






## bitmap_counter

efficient unique value counter (to be used mostly as a unique users counter) with time slots, making use of redis bitmaps.

It makes use of new redis-2.6 commands BITCOUNT and BITOP, so it will not function on redis-2.4.

###Example:

```python
from patterns.bitmap_counter import BitmapCounter

#Daily unique users counter
counter = BitmapCounter('unique_users', timeResolutions=(BitmapCounter.RES_DAY))

#sampling current user
counter.add(3)

#Getting the unique user count for today
counter.getCount((time.time(),), counter.RES_DAY)
```

###New:
It now also supports mapping of non sequential or non numeric ids to incemental ids, that makes it memory optimized.


## LuaCall

A convenience wrapper that allows you to edit, precache and call Lua scripts available in redis-2.6, as if they were native python functions.

### Example:

let mult.lua contain the code:

```lua

local val = ARGV[1]*ARGV[2]
redis.call('set', KEYS[1], val)
return redis.call('get', KEYS[1])

```

Running it from python:
```python
import redis
from patterns.lua import LuaCall, LuaScriptError
conn = redis.Redis()

#Define the call, and make it runn on our connection
mult = LuaCall(open('mult.lua'), conn)

#Call it once:
print "Result: %s" % mult(keys = ('foor',), args = (3,10))

#call it again
print "Result: %s" % mult(keys = ('foor2',), args = (5,20))

```


## idgenerator


Used in the object store, this can also be used standalone, as a centralized unique, incremental id generator using redis.
To optimize performance, it reserves in local memory many ids when accessing redis, which can be tuned.


## redis_unit

A unit-test like set of assertions about redis data to be used to validate the data inside a redis database.

###Example:

```python

from patterns.redis_unit import RedisDataTest

class MyTest(RedisDataTest):

    def testSomeStuff(self):


        self.assertPrefixCount('users:*', minAmount=100000)
        self.assertKeysExists('users:1')
        self.assertKeysType(self.test.T_HASH, 'users:1')
        self.assertHashValue('users:1', 'name', self.equals('John'))


test = MyTest('localhost', 6379)
test.run()


```


---------------------------

### To follow soon:

   * geo search


   * full text search


   * hierarchical counters


   * MySQL data sync


   * Generic expiring object cache.


# Feel free to contribute more recipes...

#Project TODO:

1. Add unit tests for all objects

2. Add geo-key


