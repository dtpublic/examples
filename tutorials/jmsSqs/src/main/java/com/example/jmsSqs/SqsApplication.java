/**
 * Put your copyright and license info here.
 */
package com.example.jmsSqs;

import javax.jms.ConnectionFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.jms.JMSBase;
import com.datatorrent.lib.io.jms.JMSStringInputOperator;

@ApplicationAnnotation(name="Sqs2HDFS")
public class SqsApplication implements StreamingApplication
{

  /**
   * AWS credential file to be used by the operator end 
   */
  public static final String SQSDEV_CREDS_FILENAME_PROPERTY = "AWS_CREDENTIALS_OPERATOR";
  
  /**
   * Queue namne to use 
   */
  public static final String QUEUE_NAME_PROPERTY = "AWS_QUEUE_NAME";
  
  static class MyConnectionFactoryBuilder implements JMSBase.ConnectionFactoryBuilder {
    
    String sqsDevCredsFilename;
    
    MyConnectionFactoryBuilder()
    {
    }
    
    @Override
    public ConnectionFactory buildConnectionFactory() 
    {
      // Create the connection factory using the properties file credential provider.
      // Connections this factory creates can talk to the queues in us-east-1 region. 
      SQSConnectionFactory connectionFactory =
          SQSConnectionFactory.builder()
          .withRegion(Region.getRegion(Regions.US_EAST_1))
          .withAWSCredentialsProvider(new PropertiesFileCredentialsProvider(sqsDevCredsFilename))
          .build();
      return connectionFactory;
    }
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
	  
    JMSStringInputOperator sqsInput = dag.addOperator("sqsIn", 
              new JMSStringInputOperator());
    
    MyConnectionFactoryBuilder factoryBuilder = new MyConnectionFactoryBuilder();
    
    factoryBuilder.sqsDevCredsFilename = conf.get(SQSDEV_CREDS_FILENAME_PROPERTY);
    
    sqsInput.setConnectionFactoryBuilder(factoryBuilder);
    sqsInput.setSubject(conf.get(QUEUE_NAME_PROPERTY));
    // for SQS ack mode should be "AUTO_ACKNOWLEDGE" and transacted = false
    sqsInput.setAckMode("AUTO_ACKNOWLEDGE");  
    sqsInput.setTransacted(false);
    
    LineOutputOperator out = dag.addOperator("fileOut", new LineOutputOperator());

    dag.addStream("data", sqsInput.output, out.input);
  }
}
