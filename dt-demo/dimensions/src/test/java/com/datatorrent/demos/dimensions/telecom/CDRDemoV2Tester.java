/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.net.URI;
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.CDRDemoV2;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

public class CDRDemoV2Tester extends CDRDemoV2
{
  private static final Logger logger = LoggerFactory.getLogger(CDRDemoV2Tester.class);

  protected long runTime = 60000;

  @Before
  public void setUp()
  {
    CustomerEnrichedInfoHBaseConfig.instance().setHost("localhost");
    EnrichedCDRHBaseConfig.instance().setHost("localhost");
    CustomerEnrichedInfoCassandraConfig.instance().setHost("localhost");
    EnrichedCDRCassandraConfig.instance().setHost("localhost");
    enableDimension = true;
  }

  @Test
  public void test() throws Exception
  {
    Configuration conf = new Configuration(false);
    conf.set(PROP_STORE_PATH, "target/temp");

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

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

  @Override
  protected PubSubWebSocketAppDataQuery createAppDataQuery()
  {
    PubSubWebSocketAppDataQuery query = new PubSubWebSocketAppDataQuery();
    //query.setTopic("telecomdemo-query");
    try {
      query.setUri(new URI("ws://localhost:9090/pubsub"));
    } catch (URISyntaxException uriE) {
      throw new RuntimeException(uriE);
    }

    return query;
  }

  @Override
  protected PubSubWebSocketAppDataResult createAppDataResult()
  {
    PubSubWebSocketAppDataResult wsOut = new PubSubWebSocketAppDataResult();
    wsOut.setTopic("telecomdemo-result");
    try {
      wsOut.setUri(new URI("ws://localhost:9090/pubsub"));
    } catch (URISyntaxException uriE) {
      throw new RuntimeException(uriE);
    }
    return wsOut;
  }
}
