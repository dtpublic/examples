/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.dimensions.telecom.generate.CustomerEnrichedInfoEmbededRepo;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoEmbededRepoTester
{
  private static final Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoEmbededRepoTester.class);

  @Test
  public void test()
  {
    CustomerEnrichedInfoEmbededRepo repo = CustomerEnrichedInfoEmbededRepo.instance();
    for (int i = 0; i < 100; ++i) {
      SingleRecord record = repo.getRandomCustomerEnrichedInfo();
      logger.info("{}", record);
    }
  }
}
