/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerServiceHBaseConf;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;

public class CustomerServiceHbaseOutputOperator extends TelecomDemoHBaseOutputOperator<CustomerService>{
  private static byte[] familyName = Bytes.toBytes("f1");
  
  public CustomerServiceHbaseOutputOperator()
  {
    setHbaseConfig(CustomerServiceHBaseConf.instance());
  }
  
  @Override
  public Put operationPut(CustomerService cs) {
    Put put = new Put(Bytes.toBytes(cs.imsi));
    put.add(familyName, Bytes.toBytes("totalDuration"), Bytes.toBytes(cs.totalDuration));
    put.add(familyName, Bytes.toBytes("wait"), Bytes.toBytes(cs.wait));
    put.add(familyName, Bytes.toBytes("zipCode"), Bytes.toBytes(cs.zipCode));
    put.add(familyName, Bytes.toBytes("issueType"), Bytes.toBytes(cs.issueType.name()));
    put.add(familyName, Bytes.toBytes("satisfied"), Bytes.toBytes(cs.satisfied));
    return put;
  }

}
