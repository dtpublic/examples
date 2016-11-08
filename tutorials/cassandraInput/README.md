# Sample Cassandra InputOperator implementation

This application reads data from cassandra database using [CassandraPOJOInputOperator](https://github.com/apache/apex-malhar/blob/master/contrib/src/main/java/com/datatorrent/contrib/cassandra/CassandraPOJOInputOperator.java) 
and writes the records to a file using [GenericFileOutputOperator](https://github.com/apache/apex-malhar/blob/master/library/src/main/java/org/apache/apex/malhar/lib/fs/GenericFileOutputOperator.java) from [Apex Malhar](https://github.com/apache/apex-malhar).


Follow these steps to run this application:

**Step 1**: Update/add these properties in the file `src/main/resources/META-INF/properties-CassandraInputApplication.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.CassandraReader.prop.store.node | cassandra server node hostname or IP address |
| dt.operator.CassandraReader.prop.store.userName | cassandra server userName |
| dt.operator.CassandraReader.prop.store.password | cassandra server password |

**Step 2**: Create database keyspace and table and add entries

Go to the console and run (where _{path}_ is a suitable prefix):

    shell> ./cqlsh -f {path}/src/main/resources/META-INF/example.cql

After this, please verify that `testapp.dt_meta` & `testapp.TestUser` tables are created and `TestUser` table has records. 

**Step 3**: Build the code:

    shell> mvn clean package

Upload the `target/cassandra-input-app-1.0.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex` cli script.

**Step 4**: During launch use `src/main/resources/META-INF/properties-CassandraInputApplication.xml` as a custom configuration file; then verify that the output by checking files created by application where table data is written.
