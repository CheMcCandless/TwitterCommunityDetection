Scalable Community Detection using Label Propagation and Map Reduce
===================

Author: Akshay Bhat 
-----------------

This is an old code and no longer useful.

When this code was written the largest AWS EC2 instance (~64Gb RAM) could barely hold the follower graph data in memory.
However this is not the case today, you can easily get a large memory (~240Gb RAM) cluster EC2 instance which allows community detection to run in-memory, thus obviating need for using hadoop.