#!/usr/bin/python
"""
Community detection using Label Propagation algorithm for Twitter

This code is written to run on a High Memory EC2 Cluster Instance

Akshay Bhat (aub3@cornell.edu)
"""
import random,collections,marshal
from multiprocessing import Pool

def maxVote(user,nLabels):
    """
    This function is used by map function, given a list of labels of neighbors
    this function finds the most frequent labels and randomly returns one of them
    # weight =  float(maxv) / len(nLabels)
    """
    cnt = collections.defaultdict(int)
    for i in nLabels:
        cnt[i] += 1
    maxv = max(cnt.itervalues())
    return user,random.choice([k for k,v in cnt.iteritems() if v == maxv])


class TwitterCommunityDetection(object):
    def __init__(self,data_path=None,cache_path=None):
        if cache_path:
            self.following,self.following_count,self.followers_count = marshal.load(file(cache_path))
        else:
            # self.following = {}
            self.followers_count = collections.defaultdict(int)
            self.following_count = collections.defaultdict(int)
            data = open(data_path).readlines() # we have so much memory why not use it
            self.count = 0
            for line in data:
                source,target = line.split('\t')
                source = int(source)
                target = int(target)
                # self.following.setdefault()(target)
                self.followers_count[target] += 1
                self.following_count[source] += 1
                self.count += 1
                if self.count % 1000000 == 0:
                    print "Loaded ",self.count," edges"
            print "Finished loading ",self.count," edges"
            data.close()
        self.Label = collections.defaultdict(int)
        self.followers_hist = collections.defaultdict(int)
        self.following_hist = collections.defaultdict(int)
        for v in self.following_count.itervalues():
            self.following_hist[v] += 1
        fh = file('/dev/shm/following_hist')
        fh.write('\n'.join(sorted(self.following_hist.items())))
        fh.close()
        print "Following histogram stored in /dev/shm"


    def detect_community(self,n,exclude_limit = 900):
        print "starting label propagation with",n,"rounds and excluding users following more than ",exclude_limit,"users"
        exclude = set([user for user,num in self.followers_count.iteritems() if num > exclude_limit])
        for k in range(n):
            self.SynchronousLabelPropagation(k,exclude)


    def SynchronousLabelPropagation(self,detection_round,exclude):
        """
        Applies the value of label for each neighbor
        and calls maxVote function
        """
        user_buffer = []
        po = Pool()
        self.LabelUpdated = collections.defaultdict(int)
        for user,following in self.following.iteritems():
            Labels = []
            for k in following:
                if k not in exclude:
                    if self.Label[k]:
                        Labels.append(self.Label[k])
                    else:
                        Labels.append(self.Label[k])
                if Labels:
                    user_buffer.append((user,Labels))
                    if len(user_buffer)> 1000000:
                        for user,new_label in po.map(maxVote,user_buffer):
                            self.LabelUpdated[user] = new_label
                        print "Processed Million users"
                        user_buffer = []
        for user,new_label in po.map(maxVote,user_buffer):
            self.LabelUpdated[user] = new_label
        self.Label = self.LabelUpdated
        po.close()
        print "finished round",detection_round
        fh = open('/dev/shm/'+str(detection_round),'w')
        fh.write('\n'.join([k+'\t'+v for k,v in self.LabelUpdated.iteritems()]))
        fh.close()
        print "stored round",detection_round





if __name__ == '__main__':
    import os
    # wget http://an.kaist.ac.kr/~haewoon/release/numeric2screen.tar.gz
    try:
        fh = open('/dev/shm/twitter_rv.net')
        fh.close()
    except IOError:
        os.system('wget http://an.kaist.ac.kr/~haewoon/release/twitter_social_graph/twitter_rv.tar.gz -P /dev/shm')
        os.system('tar xzf /dev/shm/twitter_rv.tar.gz')
        pass
    CommunityDetection = TwitterCommunityDetection('/dev/shm/twitter_rv.net')
    CommunityDetection.detect_community(10)
