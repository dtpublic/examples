# Sample Cassandra OutputOperator implementation

This application generates tests data pojo and uses [CassandraPojoOputputOperator](https://github.com/apache/apex-malhar/blob/master/contrib/src/main/java/com/datatorrent/contrib/cassandra/CassandraPOJOOutputOperator.java) 
from [Apex Malhar](https://github.com/apache/apex-malhar) library to write pojo input data to tables in cassandra database.


Follow these steps to run this application:

**Step 1**: Update/add these properties in the file `src/site/conf/properties-CassandraOutputTestApp.xml`:

| Property Name  | Description |
| -------------  | ----------- |
| dt.operator.CassandraDataWriter.prop.store.node | cassandra server node |
| dt.operator.CassandraDataWriter.prop.store.userName | cassandra server userName |
| dt.operator.CassandraDataWriter.prop.store.password | cassandra server password |

**Step 2**: Create database keyspace and table and add entries

Go to the console and run (where _{path}_ is a suitable prefix):

    shell> ./cqlsh -f {path}/src/test/resources/example.cql

After this, please verify that `testapp.dt_meta` & `testapp.TestUser` tables are created.

**Step 3**: Build the code:

    shell> mvn clean install

Upload the `target/cassandra-output-app-1.0.0-SNAPSHOT.apa` to the UI console if available or launch it from
the commandline using `apex` cli script.

**Step 4**: During launch use `site/conf/properties-CassandraOutputTestApp.xml` as a custom configuration file; then verify
that the output by checking cassandra table for newly added data.



