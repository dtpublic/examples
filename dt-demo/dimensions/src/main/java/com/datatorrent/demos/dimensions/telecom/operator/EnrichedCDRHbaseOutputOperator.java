/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;

public class EnrichedCDRHbaseOutputOperator extends TelecomDemoHBaseOutputOperator<EnrichedCDR>
{
  private static byte[] familyName = Bytes.toBytes("f1");

  public EnrichedCDRHbaseOutputOperator()
  {
    setHbaseConfig(EnrichedCDRHBaseConfig.instance());
  }

  @Override
  public Put operationPut(EnrichedCDR ecdr)
  {
    Put put = new Put(Bytes.toBytes(ecdr.getImsi()));
    put.add(familyName, Bytes.toBytes("isdn"), Bytes.toBytes(ecdr.getIsdn()));
    put.add(familyName, Bytes.toBytes("imei"), Bytes.toBytes(ecdr.getImei()));
    put.add(familyName, Bytes.toBytes("plan"), Bytes.toBytes(ecdr.getPlan()));
    put.add(familyName, Bytes.toBytes("callType"), Bytes.toBytes(ecdr.getCallType()));
    put.add(familyName, Bytes.toBytes("correspType"), Bytes.toBytes(ecdr.getCorrespType()));
    put.add(familyName, Bytes.toBytes("correspIsdn"), Bytes.toBytes(ecdr.getCorrespIsdn()));
    put.add(familyName, Bytes.toBytes("duration"), Bytes.toBytes(ecdr.getDuration()));
    put.add(familyName, Bytes.toBytes("bytes"), Bytes.toBytes(ecdr.getBytes()));
    put.add(familyName, Bytes.toBytes("dr"), Bytes.toBytes(ecdr.getDr()));
    put.add(familyName, Bytes.toBytes("lat"), Bytes.toBytes(ecdr.getLat()));
    put.add(familyName, Bytes.toBytes("lon"), Bytes.toBytes(ecdr.getLon()));
    put.add(familyName, Bytes.toBytes("date"), Bytes.toBytes(ecdr.getDate()));
    put.add(familyName, Bytes.toBytes("time"), Bytes.toBytes(ecdr.getTimeInDay()));
    if (ecdr.getDrLabel() != null) {
      put.add(familyName, Bytes.toBytes("drLabel"), Bytes.toBytes(ecdr.getDrLabel()));
    }
    put.add(familyName, Bytes.toBytes("operatorCode"), Bytes.toBytes(ecdr.getOperatorCode()));
    put.add(familyName, Bytes.toBytes("deviceBrand"), Bytes.toBytes(ecdr.getDeviceBrand()));
    put.add(familyName, Bytes.toBytes("deviceModel"), Bytes.toBytes(ecdr.getDeviceModel()));
    put.add(familyName, Bytes.toBytes("zipCode"), Bytes.toBytes(ecdr.getZipCode()));
    return put;

  }
}
