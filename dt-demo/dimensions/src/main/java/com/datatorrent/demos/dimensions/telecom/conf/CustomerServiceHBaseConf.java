/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerServiceHBaseConf extends DataWarehouseConfig
{
  private static CustomerServiceHBaseConf instance;

  public static CustomerServiceHBaseConf instance()
  {
    if (instance == null) {
      synchronized (CustomerServiceHBaseConf.class) {
        if (instance == null) {
          instance = new CustomerServiceHBaseConf();
        }
      }
    }
    return instance;
  }

  protected CustomerServiceHBaseConf()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerServiceTableName();
  }
}
