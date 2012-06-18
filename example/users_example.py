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

from patterns.object_store.objects import IndexedObject, KeySpec
from patterns.object_store.indexing import UnorderedKey, OrderedNumericalKey

import time
import logging
from hashlib import md5

class User(IndexedObject):

    __SALT = 'DFG3rg3egef92f29f2h9f7hAAA'
    _spec = ('id', 'name', 'email', 'pwhash', 'registrationDate', 'score')

    _keySpec = KeySpec(
        UnorderedKey(prefix='users',fields=('email',)),
        UnorderedKey(prefix='users',fields=('name',)),
        OrderedNumericalKey(prefix='users', field='score')
    )

    def __init__(self, **kwargs):
        IndexedObject.__init__(self, **kwargs)

        self.registrationDate = int(kwargs.get('registrationDate', time.time()))


    def _hashPTPassword(self, pw):
        md = md5()
        md.update(pw + self.__SALT)
        return md.hexdigest()


    def save(self):

        self.score = random.randint(0, 100)
        IndexedObject.save(self)

    def setPassword(self, password, doSave = False):
        self.pwhash = self._hashPTPassword(password)
        if doSave:
            self.save()


    def login(self, email, password):
        users = User.get({'email': email})
        pwhash  =self._hashPTPassword(password)
        if not users:
            logging.warn("Invalid login: %s, %s. no such email", email, password)
            return None

        user = users[0]
        if user.pwhash == pwhash:
            logging.info("Logged user %s successfully!", user)
            return user

        else:
            logging.warn("Passwords do not match. expected %s, got %s", user.pwhash, pwhash)

        return None


if __name__ == '__main__':
    import random
    import time
    from multiprocessing import Pool
    #users =  User.get(Condition({'email': user.email}))


    import PySQLPool
    def getQueryObject(**kwargs):
        """
        Get a new connection from the PySQLPool
        @return a new connection, of None if an error has occured
        """
        try:


            conn = PySQLPool.getNewConnection(host = 'localhost',
                username=  'root',
                password=  '',
                schema=  'test',
                port= 3306,
                commitOnEnd = True)
            query = PySQLPool.getNewQuery(connection = conn)

            return query
        #something went wrong
        except Exception, e:
            logging.error("Could not get query object: %s", e)
            return None


    def mysqlCreator(n):

        q = getQueryObject()

        for i in xrange(int(n)):

            uid = random.randint(1, 10000000)
            queryString = "INSERT INTO users(name, email, registrationTime) VALUES (%s, %s, now())"

            q.Query(queryString, ('User %s' % uid, 'user%s@domain.com' % uid))
            #q.Query('COMMIT')



    def creationRunner(n):
        #print "Running %d times!" % int(n)
        for i in xrange(int(n)):

            uid = random.randint(1, 10000000)
            user = User(email = 'user%s@domain.com' % uid, name = 'User %s' % uid)
            user.setPassword('q1w2e3')
            user.save()

    def getRunner(n):
        hits = 0
        for i in xrange(int(n)):

            #uid = random.randint(1, 10000000)
            user = User.loadObjects([i])

            if user:
                hits += 1
            #print user
        #print hits, '/', n

    def mysqlGetRunner(n):
        hits = 0
        q = getQueryObject()

        for i in xrange(int(n)):
            q.Query('SELECT * FROM users WHERE id=%s', (i,))

            if q.rowcount > 0:
                hits += 1
                #print user
        print hits, '/', n


    total = 0

    #creationRunner(100)

    #users = User.get(Condition({'score': Condition.Between(95, 100)}, paging= (0,1)))
    #print users
    of = open('/tmp/stats.csv', 'w+')
    for z in xrange(20):
        st = time.time()
        N = 100000#
        procs = 8
        p = Pool(procs)
        p.map(creationRunner, [N/procs] * procs)
        et = time.time()
        writeTime = et-st
        total += N
        print [total,N/writeTime ]
        of.write('%s\t%s\n' % (total,N/writeTime))
        of.flush()
#        #print "after %d: %d ops took %.03fsec, rate %.02f qps" % ((z+1) * N, N, et-st, N/(et-st))



    #print users