/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCustomerServiceHBaseConf extends DataWarehouseConfig
{
  private static EnrichedCustomerServiceHBaseConf instance;

  public static EnrichedCustomerServiceHBaseConf instance()
  {
    if (instance == null) {
      synchronized (EnrichedCustomerServiceHBaseConf.class) {
        if (instance == null) {
          instance = new EnrichedCustomerServiceHBaseConf();
        }
      }
    }
    return instance;
  }

  protected EnrichedCustomerServiceHBaseConf()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getEnrichedCustomerServiceTableName();
  }
}
