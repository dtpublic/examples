/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import com.datatorrent.demos.dimensions.telecom.model.CustomerService;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService.IssueType;

public class CustomerServiceRandomGenerator implements Generator<CustomerService>
{
  public static final int MAX_DURATION = 100;
  private ImsiGenerator imsiGenerator = new ImsiGenerator();
  private MsisdnGenerator msisdnGenerator = new MsisdnGenerator();
  private ImeiGenerator imeiGenerator = new ImeiGenerator();

  @Override
  public CustomerService next()
  {
    String imsi = imsiGenerator.next();
    String isdn = msisdnGenerator.next();
    String imei = imeiGenerator.next();

    int totalDuration = Generator.random.nextInt(MAX_DURATION);
    int wait = (int)(totalDuration * Math.random());
    String zipCode = LocationRepo.instance().getRandomZipCode();
    IssueType issueType = IssueType.values()[Generator.random.nextInt(IssueType.values().length)];
    boolean satisfied = (Generator.random.nextInt(1) == 0);
    return new CustomerService(imsi, isdn, imei, totalDuration, wait, zipCode, issueType, satisfied);
  }
}
