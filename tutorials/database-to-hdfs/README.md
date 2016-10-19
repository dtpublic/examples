## Database to HDFS App template

Ingest records from a DataBase table to hadoop HDFS. This application reads messages from configured MySQL table and writes each record as a comma separated line in HDFS files. The source code is available at: https://github.com/DataTorrent/examples/tree/master/tutorials/database-to-hdfs. Send feedback or feature requests to: feedback@datatorrent.com

Follow these steps to run the application.

1. Update these properties in the file `src/main/resources/META_INF/properties.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.JdbcPoller.prop.store.databaseUrl | database URL of the form `jdbc:mysql://hostName:portNumber/dbName` |
| dt.operator.JdbcPoller.prop.store.userName | MySQL user name |
| dt.operator.JdbcPoller.prop.store.password | MySQL user password |
| dt.operator.JdbcPoller.prop.tableName | Table to read from |
| dt.operator.JdbcPoller.prop.whereCondition | Where condition (blank for select all) |
| dt.operator.fileOutput.prop.filePath   | HDFS output directory path |
| dt.operator.fileOutput.prop.outputFileName   | HDFS output file name |

**Step 2**: Create database table and add entries

Go to the MySQL console and run (where _{path}_ is a suitable prefix):

    mysql> source {path}/src/test/resources/example.sql

After this, please verify that `testDev.test_event_table` is created and has 10 rows:

    mysql> select count(*) from testDev.test_event_table;
    +----------+
    | count(*) |
    +----------+
    |       10 |
    +----------+

**Step 3**: Create HDFS output directory if not already present (_{path}_ should be the same as specified in `META_INF/properties.xml`):

    hadoop fs -mkdir -p {path}

**Step 4**: Build the code:

    shell> mvn clean install

Upload the `target/database-to-hdfs-0.8.apa` to the UI console if available or launch it from
the commandline using `apexcli`.

**Step 5**: During launch use `-apconf cluster-memory-conf.xml` as a custom configuration file if application is being 
launched on medium to large sized cluster. Use `-apconf sandbox-memory-conf.xml` if application is being launched on 
single node or [datatorrent RTS sandbox](https://www.datatorrent.com/download/datatorrent-rts-sandbox-edition-download/). 
Then verify that the output directory has the expected output.
