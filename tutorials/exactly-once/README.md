#Examples for Exactly-Once with Apache Apex

[How does it work?](https://www.datatorrent.com/blog/end-to-end-exactly-once-with-apache-apex/)

## Read from Kafka, write to JDBC

This application shows exactly-once output to JDBC through transactions:

[Application](src/main/java/com/example/myapexapp/Application.java)

[Test](src/test/java/com/example/myapexapp/ApplicationTest.java)

## Read from Kafka, write to Files

This application shows exactly-once output to HDFS through atomic file operation:

[Application](src/main/java/com/example/myapexapp/AtomicFileOutputApp.java)

[Test](src/test/java/com/example/myapexapp/AtomicFileOutputAppTest.java)

