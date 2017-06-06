kafka-archiver-java
========
Simple backup utility that read the raw content of a Kafka topic using a high level consumer group passed as parameter, stream the output to a daily rotated file, gzip and upload the resulting files to a given S3 bucket.

I use this to backup the content of our queue on S3 to be able to replay it after.

# Example usage
```
java -jar kafka-archiver-java.jar <server> <topicName> <groupId> <workDirectory> <awsAccessKey> <awsSecret> <awsBucket> <awsPath> 
```
