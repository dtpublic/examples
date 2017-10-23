/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.app;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = TelecomDemoV2.APP_NAME)
public class TelecomDemoV2 implements StreamingApplication
{
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomDemoV2.class);

  public static final String APP_NAME = "TelecomDemoV4";

  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Cassandra = 0x100;

  protected int outputMask = outputMask_Cassandra;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    {
      CDRDemoV2 cdr = new CDRDemoV2(APP_NAME);
      cdr.setOutputMask(outputMask);
      cdr.populateDAG(dag, conf);
    }

    {
      CustomerServiceDemoV2 cs = new CustomerServiceDemoV2(APP_NAME);
      cs.setOutputMask(outputMask);
      cs.populateDAG(dag, conf);
    }
  }

  public int getOutputMask()
  {
    return outputMask;
  }

  public void setOutputMask(int outputMask)
  {
    this.outputMask = outputMask;
  }
}
