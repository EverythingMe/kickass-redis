from distutils.core import setup
setup(name='kickass_redis',
    version='0.1',
    package_dir={'kickass_redis': 'src'},
    packages=['kickass_redis', 'kickass_redis.patterns', 'kickass_redis.patterns.object_store'],
)