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

'''
This module demonstrates a very simple music library indexed by artists and titles, with full text search on track details

@author: dvirsky
'''

import redis 
import tagpy
import logging
import os
from kickass_redis.util import InstanceCache, Rediston
from kickass_redis.patterns.object_store.indexing import FullTextKey, UnorderedKey
from kickass_redis.patterns.object_store.condition import  Condition
from kickass_redis.patterns.object_store.objects import IndexedObject, KeySpec


class Track(IndexedObject):
    """
    This class represents a track
    """
    _keySpec = KeySpec(
        FullTextKey( prefix='trk', alias='trackName', fields = {'title': 10, 'artist': 5, 'album': 1 }, delimiter=' '),
        UnorderedKey(fields=('artist','album'),prefix='trk'),
        UnorderedKey(fields=('artist',), prefix='trk')
    )
    _spec = ['path', 'title', 'artist', 'album', 'year', 'genre', 'length']
    
    def __init__(self, **kwargs):

        IndexedObject.__init__(self, **kwargs)

        if hasattr(self,'path'):
            self.basename = os.path.basename(self.path or '') 
    

        
class MusicLibrary(object):
    '''

    '''


    def __init__(self, folders, redisHost = 'localhost', redisPort = 6379, redisDb = 0):
        '''
        Constructor
        @param folders a list of folders to index or monitor
        '''
        
        self.folders = folders
        Track.config(host=redisHost, port=redisPort, db=redisDb)

        
    def scanFolders(self):
        """
        Scan all the folders recursively and index the files in redis
        """
        i = 0
        for folder in self.folders:
            for root, dirs, files in os.walk(folder):
                
                for file in files:
                    
                    try:
                        self._indexFile(root, file)
                    except:
                        
                        logging.exception("Cannot index file %s", file)
                    
        
    def getAll(self, *fields):
        
        return Track.getAll(self.redis, *fields)

    def getAlbum(self, artist, album):

        return Track.get(Condition({'artist': artist, 'album': album}))

    def getArtist(self, artist):

        return Track.get(Condition({'artist': artist}))


    def findTracks(self, string):
        """
        Find tracks according to a string query
        """
        return Track.get(Condition({'trackName': string}))
    
    def get(self, *ids):
        """
        Find tracks according to a string query
        """
        return Track.loadObjects(ids)
        
        
    def _indexFile(self, root, file):
        """
        Index one track in redis based on id3 tags
        """
        path = '%s/%s' % (root, file)
        
    
        try:
            try:
                f = tagpy.FileRef(path)
            except:
                return
            t = f.tag()
            try:
                p = f.audioProperties()
                length = p.length
            except:
                length = 0
            
            
            trk = Track(path = path, title = t.title, album = t.album, artist=t.artist, year = t.year, genre = t.genre, length = length)
            trk.save()
            
        except:
            logging.exception("could not index file")

        
        
if __name__ == '__main__':
    
    
    lib = MusicLibrary(['/path/to/Music'])
    
    lib.scanFolders()
    print lib.findTracks(u'the pixies')
    print lib.getArtist(u'Metronomy')
    print lib.getAlbum(u'The Killers', u'Live From The Royal Albert Hall')
    