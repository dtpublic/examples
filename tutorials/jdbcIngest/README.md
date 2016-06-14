Sample mysql implementation

This application reads from a table in mysql, creates POJO's and then writes them to a file in the user specified directory in HDFS.

Steps :

Step 1 : Update the below properties in the properties file - src/site/conf/example.xml

1.dt.application.SimpleJdbcToHDFSApp.operator.JdbcInput.prop.store.databaseUrl
- data base URL of the form jdbc:mysql://hostName:portNumber/dbName
2.dt.application.SimpleJdbcToHDFSApp.operator.JdbcInput.prop.store.userName
- mysql user name
3.dt.application.SimpleJdbcToHDFSApp.operator.JdbcInput.prop.store.password
- password
4.dt.application.SimpleJdbcToHDFSApp.operator.FileOutputOperator.filePath
- HDFS output directory path

Step 2: Create database, table and add entries

Go to mysql console and run the below command,
mysql> source <path to > src/test/resources/example.sql

After this is done, please verify that testDev.test_event_table is created and has 10 rows.

mysql> select count(*) from testDev.test_event_table;
+----------+
| count(*) |
+----------+
|       10 |
+----------+

Step 3: Create HDFS output directory (if not present already)

hadoop fs -mkdir -p <hadoop directory path>

Step 4: Build the code,
shell> mvn clean install 

Upload the target/jdbcInput-1.0-SNAPSHOT.apa to the gateway

Step 5 : During launch use "Specify custom properties" option and select example.xml

Verification :

shell> hadoop fs -cat <hadoop directory path>/2_op.dat.* | wc -l
This should return 10 as the count


Sample Output :

hadoop fs -cat <path_to_file>/2_op.dat.0
PojoEvent [accountNumber=1, name=User1, amount=1000]
PojoEvent [accountNumber=2, name=User2, amount=2000]
PojoEvent [accountNumber=3, name=User3, amount=3000]
PojoEvent [accountNumber=4, name=User4, amount=4000]
PojoEvent [accountNumber=5, name=User5, amount=5000]
PojoEvent [accountNumber=6, name=User6, amount=6000]
PojoEvent [accountNumber=7, name=User7, amount=7000]
PojoEvent [accountNumber=8, name=User8, amount=8000]
PojoEvent [accountNumber=9, name=User9, amount=9000]
PojoEvent [accountNumber=10, name=User10, amount=1000]


