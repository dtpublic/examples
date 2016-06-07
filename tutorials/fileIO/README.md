This example shows how to extend the `AbstractFileInputOperator` and `AbstractFileOutputOperator`
to create a high performance application to copy text (line-oriented) files.

There are two operators: `FileReader` and `FileWriter` that are connected by two ports: one for
the file content and one for file start/finish control tuples.

The properties file `META-INF/properties.xml` shows how to configure the input and output
directores as well as the number of partitions.

The application can be run on an actual cluster or in local mode within your IDE by
simply running the method `ApplicationTest.testApplication()`.
