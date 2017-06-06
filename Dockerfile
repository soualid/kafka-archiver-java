FROM vixns/java8
COPY run.sh /run.sh
CMD ["/run.sh"]
COPY target/kafka-archiver-java.jar /kafka-archiver-java.jar
