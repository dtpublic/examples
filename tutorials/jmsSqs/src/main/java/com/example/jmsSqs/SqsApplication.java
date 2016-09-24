/**
 * Put your copyright and license info here.
 */
package com.example.jmsSqs;

import javax.jms.ConnectionFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
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

  static class MyConnectionFactoryBuilder implements JMSBase.ConnectionFactoryBuilder {

    String accessKeyId;
    String secretKey;
    String awsRegionName;

    MyConnectionFactoryBuilder()
    {
    }

    @Override
    public ConnectionFactory buildConnectionFactory() 
    {
      // Create the connection factory using the our own credential provider.
      // Connections this factory creates can talk to the queues in us-east-1 region. 
      
      AWSCredentialsProvider provider = 
            new AWSCredentialsProvider() {

            @Override
            public AWSCredentials getCredentials() {
              
              return new AWSCredentials() {

                @Override
                public String getAWSAccessKeyId() {
                  return accessKeyId;
                }

                @Override
                public String getAWSSecretKey() {
                  return secretKey;
                }
                
              };
            }

            @Override
            public void refresh() {
              // nothing to do
            }
            
              };
      SQSConnectionFactory connectionFactory =
          SQSConnectionFactory.builder()
          .withRegion(Region.getRegion(Regions.fromName(awsRegionName)))
          .withAWSCredentialsProvider(provider)
          .build();
      return connectionFactory;
    }
  }
  
  @Override
  public void populateDAG(DAG dag, final Configuration conf)
  {
    
    JMSStringInputOperator sqsInput = dag.addOperator("sqsIn", 
              new JMSStringInputOperator());
    
    MyConnectionFactoryBuilder factoryBuilder = new MyConnectionFactoryBuilder();
    String sqsDevCredsFilename = conf.get(SQSDEV_CREDS_FILENAME_PROPERTY);
   
    if (sqsDevCredsFilename != null) {
      PropertiesFileCredentialsProvider provider = 
          new PropertiesFileCredentialsProvider(sqsDevCredsFilename);
      factoryBuilder.accessKeyId = provider.getCredentials().getAWSAccessKeyId();
      factoryBuilder.secretKey = provider.getCredentials().getAWSSecretKey();
    } else {
      factoryBuilder.accessKeyId = conf.get("dt.operator.sqsIn.prop.aws.key.id");
      factoryBuilder.secretKey = conf.get("dt.operator.sqsIn.prop.aws.key.secret");
    }
    factoryBuilder.awsRegionName = conf.get("dt.operator.sqsIn.prop.aws.region");
    
    sqsInput.setConnectionFactoryBuilder(factoryBuilder);
    
    LineOutputOperator out = dag.addOperator("fileOut", new LineOutputOperator());

    dag.addStream("data", sqsInput.output, out.input);
  }
}
