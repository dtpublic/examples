/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.CallDetailRecordGenerateApp;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;

public class CallDetailRecordGenerateAppTester extends CallDetailRecordGenerateApp
{
  private static final Logger logger = LoggerFactory.getLogger(CallDetailRecordGenerateAppTester.class);

  protected long runTime = 60000;

  @Before
  public void setUp()
  {
    CustomerEnrichedInfoHBaseConfig.instance().setHost("localhost");
    EnrichedCDRHBaseConfig.instance().setHost("localhost");
    CustomerEnrichedInfoCassandraConfig.instance().setHost("localhost");
    EnrichedCDRCassandraConfig.instance().setHost("localhost");
  }

  @Test
  public void test() throws Exception
  {
    TelecomDemoConf.instance.setCdrDir("target/CDR");
    this.maxFileLength = 1024 * 1024;

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    Configuration conf = new Configuration(false);

    super.populateDAG(dag, conf);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(runTime);

    lc.shutdown();
  }
}
