ZooFence
==============

ZooFence is a research prototype for consistent service partitioning. This project contains part of the source code we use for our SRDS [paper](https://drive.google.com/file/d/0BwFkGepvBDQobnJ2WWtDVjNXUlE) describing our principled approach.

This tutorial has been tested on `Ubuntu 14.04`.

# Dependencies #
* JDK 7 or JDK 8
* ZooKeeper 3.4.5

# How to run the code #
We recommend that you set up a new project from existing sources in your favourite IDE (e.g., [Eclipse](http://stackoverflow.com/questions/2636201/how-to-create-a-project-from-existing-source-in-eclipse-and-then-find-it)).

# Configuration #
`zkpartitioned.config` contains a sample configuration file.

The required configuration parameters are:

* `ZOOKEEPERS`: should contain `IP:PORT` pairs for different ZooKeeper instancess; each ZooKeeper instance represents a ZooFence partition.
* `ZKADMIN`: should contain an `IP:PORT` pair corresponding to the administrative ZooKeeper deployment, which is in charge of storing command queues.
* `FLATTENING_FACTOR`: controls when flattening operations are performed.
* `REDUCTION_FACTOR`: controls how many partitions to remove during a flattening operation.

# How to test ZooFence on localhost #
1. Deploy and start ZooKeeper instances

   `wget https://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/zookeeper-3.4.5.tar.gz`
   `tar xvfz zookeeper-3.4.5.tar.gz`

   * ZooKeeper administrative:

     `cd zookeeper-3.4.5`
     `mv conf/zoo_sample.cfg conf/zoo.cfg`
     `./bin/zkServer.sh start`

   * Zookeeper 1:

     `cp -r zookeeper-3.4.5 zookeeper1 && cd zookeeper1`
     `in conf/zoo.cfg, set dataDir=/tmp/zookeeper1 and clientPort=12181`
     `./bin/zkServer.sh start`

   * Zookeeper 2:

     `cp -r zookeeper-3.4.5 zookeeper2 && cd zookeeper2`
     `in conf/zoo.cfg, set dataDir=/tmp/zookeeper2 and clientPort=12182`
     `./bin/zkServer.sh start`

   Check the [official ZooKeeper documentation](https://zookeeper.apache.org/doc/r3.1.2/zookeeperAdmin.html#sc_singleAndDevSetup) for more details on how to deploy and start ZooKeeper instances.

2. Start the executor:

   * `cd zoofence`
   * `mkdir bin`
   * `javac -cp "jars/log4j-1.2.17.jar:jars/slf4j-api-1.7.5.jar:jars/slf4j-log4j12-1.7.5.jar:jars/zookeeper-3.4.5.jar" ./src/ch/unine/*/*.java -d bin`
   * `java -cp "jars/log4j-1.2.17.jar:jars/slf4j-api.7.5.jar:jars/slf4j-log4j12-1.7.5.jar:jars/zookeeper-3.4.5.jar:bin" ch/unine/zkexecutor/LogExecutor`
   * Make sure zkpartitioned.config is accessible.

4. Start a client:

   e.g., To start `SimpleTest.java`:
   * `javac -cp "jars/log4j-1.2.17.jar:jars/slf4j-api-1.7.5.jar:jars/slf4j-log4j12-1.7.5.jar:jars/zookeeper-3.4.5.jar:bin" ./src/ch/unine/zkpartitioned/tests/*.java -d bin`
   * `java -cp "jars/log4j-1.2.17.jar:jars/slf4j-api-1.7.5.jar:jars/slf4j-log4j12-1.7.5.jar:jars/zookeeper-3.4.5.jar:bin" ch/unine/zkpartitioned/tests/SimpleTest`
   * Make sure zkpartitioned.config is accessible.

# Note #
The correspondence between ZooKeeper instances and partitions needs to be hardcoded in `MappingFunction.java`.

# Contact #

Should you have any questions, please contact: `raluca.halalai@unine.ch`.