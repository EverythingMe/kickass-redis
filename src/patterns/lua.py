import redis
from redis.exceptions import RedisError
class LuaScriptError(RedisError):
    pass


class LuaCall(object):
    """
    A class that helps you treat lua functions like they were actual functions.
    Example:
    >>> lua_str = '''
                    local val = ARGV[1]*ARGV[2]
                    redis.call('set', KEYS[1], val)
                    return redis.call('get', KEYS[1])
                '''
    >>> mult = LuaCall(lua_str, myConnection) # also acceptable: mult = LuaCall(open('mult.lua'), myConnection)
    >>> print mult(keys = ('foo',), args = (3,10))
    30

    """
    def __init__(self, sourceOrFile, redisConn = None):
        """
        construct the functin
        @param sourceOrFile either a lua string, or a reference to a file in read mode containing the source
        @param redisConn a redis connection. if not given here, you'll have to give it on each call (useful for master/slave)
        """

        self.source = sourceOrFile if type(sourceOrFile) == str else sourceOrFile.read()
        self.conn = redisConn

        #if a connection was given - try to preload the function. if not - it will have to be given later
        if self.conn:
            self.__load()

    def __call__(self,  keys=(), args=(), conn = None):

        conn = conn or self.conn
        #try to execute
        try:
            return conn.evalsha(self.sha, len(keys), *(keys + args))
        except RedisError, e:
            #check for script doesn't exist error
            if e.message.startswith('NOSCRIPT'):

                #try and reload
                self.__load(conn)

                #one more time, with feeling!
                try:

                    return conn.evalsha(self.sha, len(keys), *(keys + args))
                except redis.RedisError, e:
                    raise LuaScriptError("Could not execute lua call: %s" % e.message)
            else:
                raise LuaScriptError(e)


    def __load(self, conn = None):
        """
        Silently preload the function to redis to be used in the future
        """
        conn = conn or self.conn
        self.sha = conn.script_load(self.source)

    def isCached(self, conn = None):
        """
        Check if our function exists
        """
        conn = conn or self.conn
        return conn.script_exists(self.sha)





