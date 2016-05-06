/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.List;

import com.google.common.collect.Lists;

import com.datatorrent.demos.dimensions.telecom.model.CustomerInfo;

public class CustomerInfoRandomGenerator implements Generator<CustomerInfo>
{
  private final MsisdnGenerator msidnGenerator = new MsisdnGenerator();
  private final ImsiGenerator imsiGenerator = new ImsiGenerator();
  private final ImeiGenerator imeiGenerator = new ImeiGenerator();

  private final int[] deviceNumArray = {1, 1, 1, 1, 1, 1, 2, 2, 3};

  @Override
  public CustomerInfo next()
  {
    //most customer only have one device.
    int deviceNumIndex = Generator.random.nextInt(deviceNumArray.length);
    int deviceNum = deviceNumArray[deviceNumIndex];
    List<String> imeis = Lists.newArrayList();
    for (int i = 0; i < deviceNum; ++i) {
      imeis.add(imeiGenerator.next());
    }
    return new CustomerInfo(imsiGenerator.next(), msidnGenerator.next(), imeis);
  }
}
