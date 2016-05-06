/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.CustomerEnrichedInfoGenerateApp;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHiveConfig;

public class CustomerEnrichedInfoGenerateAppTester extends CustomerEnrichedInfoGenerateApp
{
  protected long runTime = 60000;

  @Before
  public void setUp()
  {
    CustomerEnrichedInfoHBaseConfig.instance().setHost("localhost");
    CustomerEnrichedInfoCassandraConfig.instance().setHost("localhost");
    CustomerEnrichedInfoHiveConfig.instance().setHost("localhost");
  }

  @Test
  public void test() throws Exception
  {

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
