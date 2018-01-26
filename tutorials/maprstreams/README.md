MapR Streams provides a way to deliver messages from a range of data
producer types (for instance IoT sensors, machine logs, clickstream
data) to consumers that include but are not limited to real-time or
near real-time processing applications. This uses the same API as of
Apache Kafka 0.9.
MapR-DB is a proprietary product from MapR and is supposed to use the
same API as of HBase. In fact both MapR Streams and MapR DB can be used
interchangeably with Kafka Streams and HBase respectively.
This sample application show how to read log data from a MapR Streams
using Kafka (0.9) input operator and write them out to MapR DB using
HBase output operator.

The purpose of this application is to demonstrate that the Kafka input
operator and HBase output operator in Apache Apex Malhar library can
be used for pulling data from MapR streams and writing into MapR DB.

Note: When using MapR streams or MapR DB in this application,
appropriate dependencies would need to be added in the pom.xml.
Additionally, the application needs to be configured and is not usable
as is.

A sample operator to parse JSON formatted data into POJO has been
inserted into this pipeline. Other processing operators can be
introduced depending upon the requirements.

###### MapR Streams Properties

1. Specifying topic in MapR Streams. Please note the name of topic starts
with Stream file path, followed by ":" and then Topic name.

Property > dt.application.MaprStreamsApp.operator.Streams.prop.topics
Sample Value > **/data/streams/sample-stream:sample-topic**

2. MapR Streams Clusters running at

Property > dt.application.MaprStreamsToMaprDB.operator.Streams.prop.clusters
Sample Value > **broker1.dtlab.com:9092**

###### MapR DB Properties

HBase output operator as configured in the Application.java file.
