/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import org.junit.Before;
import org.junit.Test;

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseInputOperator;

public class EnrichedCDRHbaseInputOperatorTester
{

  @Before
  public void setUp()
  {
    EnrichedCDRHBaseConfig.instance().setHost("localhost");
  }

  @Test
  public void testInternal()
  {
    EnrichedCDRHbaseInputOperator operator = new EnrichedCDRHbaseInputOperator();
    operator.setup(null);
    operator.emitTuples();
  }

}
