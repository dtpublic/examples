## JMS ActiveMQ Output Example


This application shows the use of JMS output operator by implementing AbstractJMSSinglePortOutputOperator.
The operator uses its default JMSTransactionalStore to ensure exactly once capability. AbstractJmsOutputOperator
handles every window as a transaction. In every transaction the current windowId will be saved to a topic specific
JMS meta-data queue to skip sending tuples of already committed windows.
It is the responsibility of the user to create the meta-data queue in the JMS provider.

The application generates a sequence of string numbers to be written to an ActiveMQ queue by a JMS output operator.
The output operator is preceded by a intermediate pass-through operator which will intentionally fail after a
specified amount of processed tuples, causing the operator to be redeployed and some tuples to be reprocessed.

After that the ValidationApplication can be used to read and validate the queue for having no duplicates.
It reads all messages from the queue, validates and writes the results to a HDFS validation file.


Follow these steps to run this application:

**Step 1**: Set up the ActiveMQ Broker and the Queue to read from

Unless you already have the ActiveMQ broker installed, download, install and
start it as per http://activemq.apache.org/version-5-getting-started.html.
For this example, let us assume the broker is installed on a machine with the IP address
192.168.128.142. You can access the Admin UI as
http://192.168.128.142:8161/admin/ (or http://localhost:8161/admin/ on the
machine itself).

Use the Queues tab to create **two** queues. One queue is for the actually data with a
subject of your choice. The other is used for meta-data by JmsTransactionalStore
to guarantee exactly once and needs to have the form **{subject}.metadata**.

**Step 2**: Change default properties as wished in `src/main/resources/META-INF/properties.xml`:

For example:

| Property Name  | Description |
| -------------  | ----------- |
| sequenceGenerator.prop.maxTuplesTotal | Total number of generated tuples|
| passthrough.prop.tuplesUntilKill | Number of tuples passing until it intentionally fails|
| passthrough.prop.tuplesUntilKill | Directory path used by passthrough operator for saving state information|
| subject| Name of the queue to write to or read from|
| connectionFactoryProperties.brokerURL| ActiveMQ Broker URL e.g. tcp://192.168.128.142:61616|
| validationToFile.prop.filePath | HDFS output directory path for the validation file|
| validationToFile.prop.outputFileName | Name of the validation file |

**Step 3**: Build the code and run applications:

Build the code:

    shell> mvn clean package -DskipTests

Run JmsOutputApplication. You can use ActiveMQ console to see incoming messages.
Run ValidationApplication and check output of validation file:

    shell> hdfs dfs -cat <filePath>/<outputFileName>_*

**Note:**
The application can also be tested by running `JmsApplicationTest`.
The test uses an embedded ActiveMQ broker, runs the JmsOutputApplication first followed
by the ValidationApplication writing the validation file to `target\jms-amq-output-example`.

