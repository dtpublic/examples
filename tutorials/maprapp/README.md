This sample application show how to read log data from a MapR Streams
using Kafka (0.9) input operator and write them out to MapR DB using
HBase output operator.

An operator to parse JSON formatted data into POJO has been inserted
into this pipeline. Other processing operators can be introduced
depending upon the requirements.

###### MapR Streams Properties

Specifying topic in MapR Streams. Please note the name of topic starts
with Stream file path, followed by ":" and then Topic name.
> dt.application.MaprStreamsToMaprDB.operator.Streams.prop.topics
>
> **/data/streams/sample-stream:sample-topic**

MapR Streams Cluster running at

> dt.application.MaprStreamsToMaprDB.operator.Streams.prop.clusters
>
> **broker1.dtlab.com:9092**

###### MapR DB Properties

HBase output operator properties
