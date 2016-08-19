/**
 * Put your copyright and license info here.
 */
package com.example.jmsSqs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueResult;
import com.amazonaws.services.sqs.model.PurgeQueueRequest;
import com.datatorrent.api.LocalMode;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  private static final Logger LOG = LoggerFactory.getLogger(ApplicationTest.class);
  
  private static final String FILE_NAME = "test";
  private static final String FILE_DIR  = "/tmp/FromSQS";
  private static final String FILE_PATH = FILE_DIR + "/" + FILE_NAME + ".0";     // first part     
  
  /**
   * credentials used by the operator end i.e. JMSInputOperator in SQS mode
   */
  public static final String SQS_OPERATOR_CREDS_FILENAME = "sqsOperatorCreds.properties";
  
  /**
   * credentials used by this code i.e. example end 
   */
  public static final String SQS_EXAMPLE_CREDS_FILENAME = "sqsExampleCreds.properties";
  
  private static final String QUEUE_NAME_PREFIX = "jms4Sqs";
  
  private String currentQueueName;
  
  private String currentQueueUrl;
  
  private AmazonSQSClient sqs;
  
  // test messages                                                                                                                                
  private static String[] lines =
  {
    "1st line",
    "2nd line",
    "3rd line",
    "4th line",
    "5th line",
  };

  @Test
  public void testApplication() throws Exception {
    try {
      // delete output file if it exists                                                                                                          
      File file = new File(FILE_PATH);
      file.delete();

      // Each run creates its own uniquely named queue in SQS and then deletes it afterwards.
      //  because SQS doesn't allow a deleted queue to be reused within 60 seconds
      currentQueueName = QUEUE_NAME_PREFIX + System.currentTimeMillis();
      
      createSQSClient();
      
      // write messages to SQS Queue                                                                                                            
      writeToQueue();

      // run app asynchronously; terminate after results are checked                                                                              
      LocalMode.Controller lc = asyncRun();

      // check for presence of output file                                                                                                        
      chkOutput();

      // compare output lines to input                                                                                                            
      compare();
      
      lc.shutdown();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  private void createSQSClient() 
  {
    PropertiesFileCredentialsProvider file = new PropertiesFileCredentialsProvider(SQS_EXAMPLE_CREDS_FILENAME);
    PropertiesCredentials testCreds = (PropertiesCredentials)file.getCredentials();
    sqs = new AmazonSQSClient(testCreds);
  }

  private void writeMsg(String[] msgs) 
  {
    CreateQueueResult res = sqs.createQueue(currentQueueName);
    
    currentQueueUrl = res.getQueueUrl();
    
    // we should purge the queue first
    PurgeQueueRequest purgeReq = new PurgeQueueRequest(currentQueueUrl);
    sqs.purgeQueue(purgeReq);
    for (String text : msgs) {
      sqs.sendMessage(currentQueueUrl, text);
    }
  }
  
  private void writeToQueue() {
    writeMsg(lines);
    LOG.debug("Sent messages to topic {}", QUEUE_NAME_PREFIX);
  }

  private Configuration getConfig() {
    Configuration conf = new Configuration(false);
    conf.set(SqsApplication.SQSDEV_CREDS_FILENAME_PROPERTY, SQS_OPERATOR_CREDS_FILENAME);
    conf.set(SqsApplication.QUEUE_NAME_PROPERTY, currentQueueName);

    String pre = "dt.operator.fileOut.prop.";
    conf.set(   pre + "filePath",        FILE_DIR);
    conf.set(   pre + "baseName",        FILE_NAME);
    conf.setInt(pre + "maxLength",       50);
    conf.setInt(pre + "rotationWindows", 10);

    pre = "dt.operator.sqsIn.prop.";
    conf.set(   pre + "subject",        currentQueueName);
    // for SQS ack mode should be "AUTO_ACKNOWLEDGE" and transacted = false
    conf.set(   pre + "ackMode",        "AUTO_ACKNOWLEDGE");
    conf.setBoolean(   pre + "transacted",        false);

    return conf;
  }
  
  private static void chkOutput() throws Exception {
    File file = new File(FILE_PATH);
    final int MAX = 60;
    for (int i = 0; i < MAX && (! file.exists()); ++i ) {
      LOG.debug("Sleeping, i = {}", i);
      Thread.sleep(1000);
    }
    if (! file.exists()) {
      String msg = String.format("Error: %s not found after %d seconds%n", FILE_PATH, MAX);
      throw new RuntimeException(msg);
    }
  }

  private void compare() throws Exception {
    // read output file                                                                                                                           
    File file = new File(FILE_PATH);
    BufferedReader br = new BufferedReader(new FileReader(file));

    HashSet<String> set = new HashSet<String>();
    String line;
    while (null != (line = br.readLine())) {
      set.add(line);
    }
    br.close();

    // now delete the file, we don't need it anymore
    Assert.assertTrue("Deleting "+file, file.delete());

    // delete the current queue, since the Queue's job is done
    sqs.deleteQueue(currentQueueUrl);

    // compare                                                                                                                                    
    Assert.assertEquals("number of lines", lines.length, set.size());
    for (int i = 0; i < lines.length; ++i) {
      Assert.assertTrue("set contains "+lines[i], set.remove(lines[i]));
    }
  }

  private LocalMode.Controller asyncRun() throws Exception {
    Configuration conf = getConfig();
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new SqsApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    return lc;
  }

  
}
