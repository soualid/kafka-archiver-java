#!/bin/sh
#Usage: App <server> <topicName> <groupId> <workDirectory> <awsAccessKey> <awsSecret> <awsBucket> <awsPath> 
cd /
exec java $JAVA_OPTIONS -jar kafka-archiver-java.jar $SERVER $TOPIC $GROUP $WORKDIR $AWS_ACCESS $AWS_SECRET $AWS_BUCKET $AWS_PATH $EARLIEST
