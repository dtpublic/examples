| Application       | Description |
| ----------------- | ----------- |
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
| jdbcIngest        | Shows how to read rows from a table in an SQL database, polling and non-polling fashion, and write them to a file in HDFS. |
| jdbcToJdbc        | This application reads from an input table using JDBC, converts input to user defined POJO & then writes those POJOS to another table. |
| kafka             | Shows how to read from Kafka using the new 0.9 input operator and write to HDFS using rolling output files. |
| operatorTutorial  | Simple example of a word-count operator and a unit test for it |
| parser            | Examples showing how to use different parsers and formatters |
| partition         | Shows use of custom partitioning with a stream codec.
| recordReader      | Shows use of FSRecordReaderModule to read newline delimited records from a file. |
| topnwords         | The Top-N words example. |

