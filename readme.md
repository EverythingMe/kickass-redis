Kickass-Redis - a loose framework of Redis based data solutions
==================================================

This project aims to create a repository of useful python libraries built on top of redis (and using each other),
to automate data modeling with Redis.

Redis is relatively low level, and while it is simple to start using, getting a good knowledge of how to model problems
with it in an efficient way can be tricky. So I've created this project to wrap common use cases, into a loose framework
of redis based solutions for real world problems.


#Components
----------

To kick things off, the framework includes the following components:

## object_store

a fast yet simple ORM (well, OM actually) that automates creation, indexing and searching for complex objects using redis.

###Example:

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






## bitmap_counter

efficient unique value counter (to be used mostly as a unique users counter) with time slots, making use of redis bitmaps.

It makes use of new redis-2.6 commands BITCOUNT and BITOP, so it will not function on redis-2.4.

###Example:

    from patterns.bitmap_counter import BitmapCounter

    #Daily unique users counter
    counter = BitmapCounter('unique_users', timeResolutions=(BitmapCounter.RES_DAY))

    #sampling current user
    counter.add(3)

    #Getting the unique user count for today
    counter.getCount((time.time(),), counter.RES_DAY)


## idgenerator


Used in the object store, this can also be used standalone, as a centralized unique, incremental id generator using redis.
To optimize performance, it reserves in local memory many ids when accessing redis, which can be tuned.




---------------------------

### To follow soon:

   * geo search


   * full text search


   * hierarchical counters


   * MySQL data sync


   * Generic expiring object cache.


# Feel free to contribute more recipes...
