| Application       | Description |
| ----------------- | ----------- |
| cassandra-input-app | Example to show how to use Cassandra Input Operator.|
| cassandra-output-app | Example to show how to use Cassandra Output Operator.|
| csvformatter         | Example to show how to use CsvFormatter to format POJOs to a delimited string.|
| dedup | Example showing use of Dedup operator |
| dynamic-partition | Example showing use of StatsListener with dynamic paritioning |
| enricher          | Shows how to enrich streaming data using external source |
| exactly-once      | Shows how to read from Kafka and write to JDBC or HDFS with exactly-once semantics |
| fileIO            | Shows how to implement high-performance file copy using partitioning and the file input and output operators. Also shows the use of a second port to send control information. |
| fileIO-multiDir   | Shows how to use the file input operator to monitor multiple directories by using custom partitioning |
| fileIO-simple     | Simple example to copy data from files in an input directory to rolling files in an output directory |
| fileOutput        | Show how to use partitioning on both the file output operator and the input operator |
| fileToJdbc        | Shows how to read files from HDFS, parse into POJOs and then insert into a table in MySQL.  |
| hdht              | Shows how to use the HDHT operator. |
| hdfs2kafka        | Shows how to read from HDFS and write to a Kafka topic. |
| innerjoin         | Shows how to use streaming innerjoin operator|
| jdbcIngest        | Shows how to read rows from a table in an SQL database, polling and non-polling fashion, and write them to a file in HDFS. |
| jdbcToJdbc        | This application reads from an input table using JDBC, converts input to user defined POJO & then writes those POJOS to another table. |
| jmsActiveMQ       | Shows how to use the JMS input operator to read from an ActiveMQ queue |
| jmsSqs            | Shows how to use the JMS input operator to read from an SQS queue |
| kafka             | Shows how to read from Kafka using the new 0.9 input operator and write to HDFS using rolling output files. |
| kinesisInput      | Shows how to read from Kinesis Streams using Kinesis Input Operator and write to HDFS using rolling output files. |
| maprapp           | Shows how to read from MapR Streams using Kafka 0.9 input operator and write to MapR DB using HBase output operator. |
| operatorTutorial  | Simple example of a word-count operator and a unit test for it |
| parser            | Examples showing how to use different parsers and formatters |
| partition         | Shows use of custom partitioning with a stream codec.
| recordReader      | Shows use of FSRecordReaderModule to read newline delimited records from a file. |
| s3output          | Shows how to read files from HDFS and upload into S3 bucket.|
| s3-to-hdfs-sync   | Copying files from Amazon S3 to HDFS |
| s3-tuple-output   | Writing tuples to Amazon S3.  |
| throttle          | Shows use of OperatorRequest and StatsListener to modulate the speed of the input operator when downstream operators are unable to keep up. |
| topnwords         | The Top-N words example. |
| transform         | Example for Transform Operator. |
