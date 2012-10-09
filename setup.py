from distutils.core import setup
setup(name='kickass_redis',
    version='0.1.5',
    package_dir={'kickass_redis': 'src'},
    packages=['kickass_redis', 'kickass_redis.patterns', 'kickass_redis.patterns.object_store'],
    requires=['redis(>=2.7.1)'],
    url='https://github.com/EverythingMe/kickass-redis',
    author='Dvir Volk',
    description= 'A loose framework of kick-ass redis patterns',
    long_description="""================================================================
Kickass-Redis - a loose framework of Redis based data solutions
================================================================

This project aims to create a repository of useful python libraries built on top of redis (and using each other), to automate data modeling with Redis.

Redis is relatively low level, and while it is simple to start using, getting a good knowledge of how to model problems with it in an efficient way can be tricky. So I've created this project to wrap common use cases, into a loose framework of redis based solutions for real world problems.

Components:
==================

1. Object Store
~~~~~~~~~~~~~~~~

a fast yet simple ORM (well, OM actually) that automates creation, indexing and searching for complex objects using redis.

Indexes include: simple string index, numeric index that supports sorting and ranges, simplistic full text index, and a unique key.

2. Bitmap Counter
~~~~~~~~~~~~~~~~~

efficient unique value counter (to be used mostly as a unique users counter) with time slots, making use of redis bitmaps.

It makes use of new redis-2.6 commands BITCOUNT and BITOP, so it will not function on redis-2.4.

3. LuaCall
~~~~~~~~~~~~~~~~~

A convenience wrapper that allows you to edit, precache and call Lua scripts available in redis-2.6, as if they were native python functions.

4. Id Generator
~~~~~~~~~~~~~~~~~~

Used in the object store, this can also be used standalone, as a centralized unique, incremental id generator using redis. To optimize performance, it reserves in local memory many ids when accessing redis, which can be tuned.

5. Redis Unit
~~~~~~~~~~~~~~~~~

A unit-test like set of assertions about redis data to be used to validate the data inside a redis database.

"""
)
