/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.demos.dimensions.telecom.app.TelecomDimensionsDemo;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

public class TelecomDimensionsDemoTester extends TelecomDimensionsDemo
{
  private static final Logger logger = LoggerFactory.getLogger(TelecomDimensionsDemoTester.class);
  protected long runTime = 60000;

  @Test
  public void test() throws Exception
  {
    EnrichedCDRHBaseConfig.instance().setHost("localhost");

    Configuration conf = new Configuration(false);
    conf.set(TelecomDimensionsDemo.PROP_STORE_PATH, "target/temp");

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
    query.setTopic("telecomdemo-query");
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
