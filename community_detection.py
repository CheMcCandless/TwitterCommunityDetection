#!/usr/bin/python
"""
Community detection using Label Propagation algorithm for Twitter

Akshay Bhat (aub3@cornell.edu)
"""
import random,collections,gzip
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
    def __init__(self,data_path):
        self.following=collections.defaultdict(list)
        self.followers_count = collections.defaultdict(int)
        self.following_count = collections.defaultdict(int)
        self.Label = collections.defaultdict(int)
        data = gzip.open(data_path)
        for line in data:
            print line
            source,target = line.split('\t')
            source = int(source)
            target = int(target)
            self.following[source].append(target)
            self.followers_count[target] += 1
            self.following_count[source] += 1
        data.close()
        print "loaded data"


    def detect_community(self,n,exclude_limit = 900):
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
        for user,new_label in po.map(maxVote,user_buffer):
            self.LabelUpdated[user] = new_label
        self.Label = self.LabelUpdated
        po.close()
        fh = open(str(detection_round),'w')
        fh.write('\n'.join([k+'\t'+v for k,v in self.LabelUpdated.iteritems()]))
        fh.close()





if __name__ == '__main__':
    import os
    # wget http://an.kaist.ac.kr/~haewoon/release/numeric2screen.tar.gz
    try:
        fh = open('/dev/shm/twitter_rv.tar.gz')
        fh.close()
    except IOError:
        os.system('wget http://an.kaist.ac.kr/~haewoon/release/twitter_social_graph/twitter_rv.tar.gz -P /dev/shm')
        pass
    CommunityDetection = TwitterCommunityDetection('/dev/shm/twitter_rv.tar.gz')
    CommunityDetection.detect_community(10)
