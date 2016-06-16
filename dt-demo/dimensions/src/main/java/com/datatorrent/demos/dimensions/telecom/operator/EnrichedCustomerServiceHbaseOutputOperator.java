/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCustomerServiceHBaseConf;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;

public class EnrichedCustomerServiceHbaseOutputOperator extends TelecomDemoHBaseOutputOperator<EnrichedCustomerService>
{
  private static byte[] familyName = Bytes.toBytes("f1");

  public EnrichedCustomerServiceHbaseOutputOperator()
  {
    setHbaseConfig(EnrichedCustomerServiceHBaseConf.instance());
  }

  @Override
  public Put operationPut(EnrichedCustomerService ecs)
  {
    Put put = new Put(Bytes.toBytes(ecs.imsi));
    put.add(familyName, Bytes.toBytes("totalDuration"), Bytes.toBytes(ecs.totalDuration));
    put.add(familyName, Bytes.toBytes("wait"), Bytes.toBytes(ecs.wait));
    put.add(familyName, Bytes.toBytes("zipCode"), Bytes.toBytes(ecs.zipCode));
    put.add(familyName, Bytes.toBytes("issueType"), Bytes.toBytes(ecs.issueType.name()));
    put.add(familyName, Bytes.toBytes("satisfied"), Bytes.toBytes(ecs.satisfied));
    put.add(familyName, Bytes.toBytes("satisfied"), Bytes.toBytes(ecs.operatorCode));
    put.add(familyName, Bytes.toBytes("satisfied"), Bytes.toBytes(ecs.deviceBrand));
    put.add(familyName, Bytes.toBytes("satisfied"), Bytes.toBytes(ecs.deviceModel));
    return put;
  }

}
