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


"""
Experimental redis sentinel client.
redis sentinel is an HA agent that monitors a group or groups of master/slaves redis arrays, does automatic
Failover, and notifies clients, that can reconfigure. This client is supposed to enable automatic reconfiguration.
See  http://antirez.com/post/redis-sentinel-beta-released.html for more infor sentinel

Right now what it does is simply listen to a sentinel and monitor the cluster status.
The next step is going to be connecting it to an internal in-code load balancer, that can do automatic failover
based on the cluster state.

I'd like to try and exploit the sentinel as a sort of configuration manager for a redis cluster, replacing
Zookeper & friends  - all a client needs to know is how to connect to a sentine, and everything from there on
happens automagically.

to run the client, simply point it to a sentinel and start it. it will bootstrap to the current state and log the
changes and new state of the all the clusters
"""


import redis
from threading import Thread
import logging

class Node(object):
    """
    This encapsulates a redis node in a cluster and its status (currently only up/down)
    """
    Master = 'master'
    Slave = 'slave'
    Sentinel = 'sentinel'

    Up = 'up'
    Down = 'down'

    def __init__(self, nodeType, **kwargs):

        self.type = nodeType
        self.ip = kwargs.get('ip')
        self.port = int(kwargs.get('port'))
        self.status = Node.Up

    def __repr__(self):
        return '%s(%s:%s %s)' % (self.type, self.ip, self.port, self.status)

    def __eq__(self, other):

        return self.ip == other.ip and self.port == other.port

    def setStatus(self, st):
        if st in (Node.Up, Node.Down):
            logging.info("Setting node %s status to %s", self, st)
            self.status = st
        else:
            raise ValueError("Invalid status set for node %s: '%s'" % (self, st))

    def key(self):
        return '%s:%s' % (self.ip,self.port)


class Cluster(object):
    """
    This encapsulates a named cluster (a "master" in the sentinel internal terms)
    it has one master (that may be down) and slaves
    """
    def __init__(self, name):
        self.name = name
        self.master = None
        self.slaves = {}


    def __repr__(self):

        return "Cluster %s: master %s, slaves: %s" % (self.name, self.master, self.slaves.values())


class SentinelMessage(object):

    """
    Encapsulation for a pubsub message from the sentinel
    """


    def __init__(self, eventType, rawString):

        self.type = eventType
        self.raw = rawString

        self.__parseMessage()

    def __parseMessage(self):
        """
        Parse a pubsub message from redis
        the format is
        <instance type> <instance name> <ip> <port> [If the instance type is not master: @ <master name> <master ip> <master port> ]
        """

        parts =  [x.strip().split(' ') for x in self.raw.partition('@')]
        if self.type == '+switch-master':
            self.node =  Node(nodeType=Node.Master, ip=parts[0][-2], port=parts[0][-1])
            self.clusterName, self.masterIp, self.masterPort = parts[0][0], parts[0][-2], parts[0][-1]
        else:
            self.node = Node(nodeType=parts[0][0], ip=parts[0][2], port=parts[0][3])

            if parts[1] and parts[1][0]:

                self.clusterName, self.masterIp, self.masterPort = parts[2][0:3]
            else: #this is the master, just copy from regular fields
                self.clusterName, self.masterIp, self.masterPort = parts[0][1:4]

    def __repr__(self):
        return 'SentinelEvent(%s => %s @ %s)' % (self.type, self.node,  self.clusterName)


class SentinelClient(object):
    """
    The client that talks to the sentinel
    """
    def __init__(self, sentinelHost = 'localhost', sentinelPort = 26379):
        self.host = sentinelHost
        self.port = sentinelPort
        self.conn = redis.Redis(self.host, self.port)

        self.running = False
        self.handlers =  { }
        self.clusters = { }


    def _loadMasters(self):
        """
        Reload the list of masters and clusters
        """
        lst = self.conn.execute_command('SENTINEL', 'MASTERS')
        for record in lst:

            masterInfo = dict((record[x], record[x+1]) for x in xrange(0, len(record), 2))

            master = Node(nodeType=Node.Master, **masterInfo)

            self.clusters.setdefault(masterInfo['name'], Cluster(masterInfo['name'])).master = master

    def _loadSlaves(self):
        """
        Reload the slaves for all active clusters
        """
        for name, cluster in self.clusters.iteritems():
            cluster.slaves = {}
            lst = self.conn.execute_command('SENTINEL', 'SLAVES', name)

            for record in lst:
                d = dict((record[x], record[x+1]) for x in xrange(0, len(record), 2))
                slave = Node(nodeType=Node.Slave, **d)
                cluster.slaves[slave.key()] = slave



    def _loadSentinels(self):
        """
        TODO: Load a list of the sentinels, we don't need it yet
        """
        pass

    def reloadState(self):
        """
        Reload everything
        """
        self._loadMasters()
        self._loadSlaves()
        self._loadSentinels()
        logging.info("Reloaded all the clusters. current state: %s", self.clusters)

    def reset(self):
        """
        Send the reset command to the sentinel
        """
        self.conn.execute_command('SENTINEL', 'RESET')


    def subscribe(self, event, callback):
        """
        TODO: Allow external registration into events
        """
        pass

    def _onSlave(self, event):
        """
        Called when a slaved joins the sentinel
        """

        logging.info("Adding slave to cluster %s" % event.clusterName)
        self.clusters[event.clusterName].slaves[event.node.key()] = event.node

    def _onDown(self, event):
        """
        Called when we get an +SDOWN/ODOWN message and mark a node as down
        """
        logging.info("Node %s is going down in cluster %s", event.node, event.clusterName)

        #if it's s slave (which shouldn't happen BTW) - just mark it as down
        if event.node.key() in self.clusters[event.clusterName].slaves:

            self.clusters[event.clusterName].slaves[event.node.key()].setStatus(Node.Down)

        elif event.node ==  self.clusters[event.clusterName].master:
            self.clusters[event.clusterName].master.setStatus(Node.Down)

    def _onDownEnd(self, event):
        """
        Called when we get a -SDOWN/ODOWN message and marks a node as up
        """
        logging.info("Node %s is going UP in cluster %s", event.node, event.clusterName)


        if event.node.key() in self.clusters[event.clusterName].slaves:

            self.clusters[event.clusterName].slaves[event.node.key()].setStatus(Node.Up)

        elif event.node ==  self.clusters[event.clusterName].master:
            self.clusters[event.clusterName].master.setStatus(Node.Up)



    def _onSwitchMaster(self, event):

        newMaster = event.node
        logging.info("Switched master for cluster %s", event.clusterName)
        self.clusters[event.clusterName].master = newMaster


        if newMaster.key() in self.clusters[event.clusterName].slaves:
            del self.clusters[event.clusterName].slaves[newMaster.key()]



    def start(self):

        """
        Run the reporting loop
        """

        self.running = True
        while self.running:
            try:
                #subscribe...
                self.pubsub =self.conn.pubsub()
                self.pubsub.psubscribe('*')

                listener = self.pubsub.listen()
                while self.running:

                    event = listener.next()
                    message = SentinelMessage(event['channel'], event['data'])

                    logging.info("GOT MESSAGE %s",message)
                    #handle messages

                    #A slave was added
                    if message.type == '+slave':
                        self._onSlave(message)
                    #A node is down (slaves will never receive odown)
                    elif message.type in ('+odown', '+sdown'):
                        self._onDown(message)
                    #a node is back up
                    elif message.type in ('-odown', '-sdown'):
                        self._onDownEnd(message)
                    #master switch. YAY!
                    elif message.type == '+switch-master':
                        self._onSwitchMaster(message)
                    else:
                        #everything else we just log
                        logging.info("Unhadlend message %s", message)

                    #print the cluster state
                    logging.debug("Clusters: %s", self.clusters)

            except (SystemExit, KeyboardInterrupt):
                logging.info("we need to exit!")
                self.running = False
                break
            except:
                logging.exception("Could not process event")


    def  stop(self):

        self.running = False
        raise SystemExit()



if __name__ == '__main__':

    client = SentinelClient('localhost', 26379)
    logging.basicConfig(level = 0)

    import time
    cont = True
    while cont:
        try:
            client.reloadState()
            client.start()
            cont = False
        except:

            time.sleep(0.5)








