/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

import static org.junit.Assert.fail;

/**
 * This test case will run the JmsOutputApplication and the ValidationApplication
 * simultaneously. An embedded ActiveMQ JMS broker is started at the beginning of the test.
 * The ValidationApplication will be continuously put to sleep until the ValidationToFile operator
 * is done with the validation and his 'isValidated' flag it set to true.
 */
public class JmsApplicationTest
{
  private static final String FILE_DIR = "target/jms-amq-output-example";

  private BrokerService broker;
  private Configuration conf;

  private static final Logger logger = LoggerFactory.getLogger(JmsApplicationTest.class);

  //remove '@After' to keep validation output file
  @Before
  @After
  public void cleanup()
  {
    FileUtils.deleteQuietly(new File(FILE_DIR));
  }

  @Test
  public void testApplication() throws IOException, Exception
  {
    try {
      startEmbeddedActiveMQBroker();
      getConfig();
      runApplications();
      broker.stop();
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
    checkOutput();
  }

  private void runApplications() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    lma.prepareDAG(new JmsOutputApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.runAsync();
    Thread.sleep(8000);
    lc.shutdown();


    LocalMode lma2 = LocalMode.newInstance();
    lma2.prepareDAG(new ValidationApplication(), conf);
    ValidationToFile validationToFile = (ValidationToFile)lma2.getDAG().getOperatorMeta("validationToFile").getOperator();
    LocalMode.Controller validationController = lma2.getController();
    validationController.runAsync();
    int count = 1;
    int maxSleepRounds = 300;
    while (!validationToFile.isValidated) {
      logger.info("Sleeping ....");
      Thread.sleep(500);
      if (count > maxSleepRounds) {
        fail("isValidated did not get set to true in ValidationToFile operator");
      }
      count++;
    }
    validationController.shutdown();
  }

  /**
   * Start the embedded Active MQ broker for our test.
   *
   * @throws Exception
   */
  private void startEmbeddedActiveMQBroker() throws Exception
  {
    broker = new BrokerService();
    String brokerName = "ActiveMQOutputOperator-broker";
    broker.setBrokerName(brokerName);
    broker.getPersistenceAdapter().setDirectory(new File("target/activemq-data/" +
      broker.getBrokerName() + '/' +
      org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter.class.getSimpleName()).getAbsoluteFile());
    broker.addConnector("tcp://localhost:61616?broker.persistent=false");
    broker.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 1024);  // 1GB
    broker.getSystemUsage().getTempUsage().setLimit(100 * 1024 * 1024);    // 100MB
    broker.setDeleteAllMessagesOnStartup(true);
    broker.start();
  }

  private void getConfig()
  {
    conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.set("dt.application.JmsValidationApplication.operator.validationToFile.prop.filePath", FILE_DIR);
    conf.set("dt.application.JmsOutputApplication.operator.passthrough.prop.directoryPath", FILE_DIR);
  }

  private void checkOutput() throws IOException
  {
    String validationOutput;
    File folder = new File(FILE_DIR);

    FilenameFilter filenameFilter = new FilenameFilter()
    {
      @Override
      public boolean accept(File dir, String name)
      {
        if (name.split("_")[0].equals("validation.txt")) {
          return true;
        }
        return false;
      }
    };
    File validationFile = folder.listFiles(filenameFilter)[0];
    try (FileInputStream inputStream = new FileInputStream(validationFile)) {
      validationOutput = IOUtils.toString(inputStream);
      logger.info("Validation output: {}", validationOutput);
    }

    Assert.assertTrue(validationOutput.contains("Validation successful. No duplicate messages."));
  }

}
