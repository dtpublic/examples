/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.EnrichCDRApp;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;

public class EnrichCDRAppTester extends EnrichCDRApp
{
  private static final Logger logger = LoggerFactory.getLogger(EnrichCDRAppTester.class);
  protected long runTime = 60000;

  @Test
  public void test() throws Exception
  {
    EnrichedCDRHBaseConfig.instance().setHost("localhost");
    TelecomDemoConf.instance.setCdrDir("target/CDR");

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
