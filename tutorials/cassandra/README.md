##Example application to use Apache Apex Malhar opearators
######1. CassandraPojoOutputOperator
######2. CassandraPojoInputOperator

####Application Flow
1. TuplesDataGenerator Operator generates test tuples in the form of pojo object (TestUsers)
2. CassandraDataPopulator Operator writes the test tuples to the cassandra database.
3. CassandraDataReader Operator reads test tuples from cassandra database. The reader operator gets activated after CassandraDataPopulator is done writing all data to database. CassandraDataPopulator sends a trigger to CassandraDataReader to start read operation.
4. DataValidator operator receives tuples from CassandraDataReader, it also receives data checksum of input data and does the data validation.

####Configuration
The config file is bundled with package and is available in src/sit/conf/ folder. Update following properties:

1. Node: Update property to use your cassandra cluster
2. keyspace: Update property to use your cassandra keyspace (the keyspace should be already present on cluster)

**Update other properties as per requirements.**
