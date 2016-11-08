/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoHbaseOutputOperator extends TelecomDemoHBaseOutputOperator<SingleRecord>
{
  private static byte[] familyName = Bytes.toBytes("f1");

  public CustomerEnrichedInfoHbaseOutputOperator()
  {
    setHbaseConfig(CustomerEnrichedInfoHBaseConfig.instance());
  }

  @Override
  public Put operationPut(SingleRecord cei)
  {
    Put put = new Put(Bytes.toBytes(cei.imsi));
    put.add(familyName, Bytes.toBytes("id"), Bytes.toBytes(cei.id));
    put.add(familyName, Bytes.toBytes("imsi"), Bytes.toBytes(cei.imsi));
    put.add(familyName, Bytes.toBytes("isdn"), Bytes.toBytes(cei.isdn));
    put.add(familyName, Bytes.toBytes("imei"), Bytes.toBytes(cei.imei));
    put.add(familyName, Bytes.toBytes("operatorName"), Bytes.toBytes(cei.operatorName));
    put.add(familyName, Bytes.toBytes("operatorCode"), Bytes.toBytes(cei.operatorCode));
    put.add(familyName, Bytes.toBytes("deviceBrand"), Bytes.toBytes(cei.deviceBrand));
    put.add(familyName, Bytes.toBytes("deviceModel"), Bytes.toBytes(cei.deviceModel));

    return put;
  }

}
