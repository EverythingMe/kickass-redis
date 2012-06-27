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

from patterns.object_store.objects import  IndexedObject, KeySpec
from patterns.object_store.indexing import *
import math
import random


class Option(IndexedObject):

    _spec = ('id', 'name', 'playCount', 'reward', 'testId')

    _keySpec = KeySpec(
        OrderedNumericalKey('opt', 'testId'),
        UnorderedKey('opt', ('name','testId'))
    )

    __algo = None

    def __init__(self, name = '', playCount = 0, reward = 0, testId = 0, **kwargs):

        IndexedObject.__init__(self,
                                name = name,
                                playCount = int(playCount),
                                reward = int(reward),
                                testId = int(testId),
                                **kwargs)

    @classmethod
    def addReward(cls, testId, name, amount = 1):
        cls.incrementWhere(Condition({'testId': testId, 'name': name}), 'reward', amount)



class Bandit(IndexedObject):

    _spec = ('id', 'name', 'benchmarkLen')

    ALGO_UCB1 = 'ucb1'
    ALGO_EPSILON_GREEDY = 'epsgreedy'

    def __init__(self, name, algo = ALGO_UCB1, benchmarkLen = 60, **kwargs):


        IndexedObject.__init__(self,
                                algo = algo,
                                name = name,
                                benchmarkLen = int(benchmarkLen),
                                **kwargs)
        self.options = []

        #set the algorithm
        if algo == self.ALGO_UCB1:
            self.algo = self._ucb1
        else:
            self.algo = self._epsilon_greedy


    def addOption(self, name):

        opt = Option(name = name, testId = self.id)
        opt.save()
        self.options.append(opt)

    def loadOptions(self, noCache = False):

        if not noCache and self.options:
            return len(self.options)

        self.options = Option.get(Condition({'testId': self.id}))

        return len(self.options)


    def selectOptionByScore(self,noCache = False):

        if not self.loadOptions(noCache):
            raise RuntimeError("Could not load any options for test %s" % self)



        self.totalPlays = sum((opt.playCount for opt in self.options))
        selected = max(self.options, key = self.algo)

        #increment option playcount
        selected.update(playCount = selected.playCount + 1)
        return selected



    def rewardOption(self, option, amount = 1):

        option.update(reward = option.reward + amount)



    def _ucb1(self, option):
        '''
        the actual ucb1 algorithm doesn't start before the preliminary benchmark is done,
        will keep pop options from a list, according to the definition done using init_test,
        until the list if empty.
        after benchmarking, will return the option with the current max ucb score.
        '''

        #for the benchmark rounds - just find the option with the smallest playCount
        if self.totalPlays < len(self.options) * self.benchmarkLen:
            return -option.playCount

        avg_score = float(option.reward) / option.playCount
        return avg_score + math.sqrt(2*math.log(self.totalPlays) / float(option.playCount))

    def _epsilon_greedy(self,  option):
        '''
        naive bandit implementation, will always explore 10% of the times.
        '''
        choice = None
        #sometimes... you just want a random option, man
        if random.random() < 0.1:
            return random.randint(0, len(self.options)**2)

        else:

            return (float(option.reward) / option.playCount) if option.playCount else 0



if __name__ == '__main__':

    bandito = Bandit.createNew(name = 'test_foo%d' % random.randint(1, 1000000000),
                                algo=Bandit.ALGO_EPSILON_GREEDY,
                                benchmarkLen=100)


    for i in xrange(3):

        bandito.addOption('option_%d' % i)


    rounds = 0
    import time
    while rounds < 10000:

        option = bandito.selectOptionByScore(noCache = True)


        r = random.randint(1,10)

        idx = bandito.options.index(option) + 2

        if r%idx ==0 or r % 3 == 0:
            #print "Rewarding option %d" % (idx - 1)

            bandito.rewardOption(option, 1)



        if rounds == bandito.benchmarkLen or rounds % 1000 == 0:
            print "After %d rounds" % rounds
            for option in bandito.options:
                print option
            print "-----------------------------------"

        rounds += 1




