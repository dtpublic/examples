/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerServiceCassandraConf extends DataWarehouseConfig
{
  private static CustomerServiceCassandraConf instance;

  public static CustomerServiceCassandraConf instance()
  {
    if (instance == null) {
      synchronized (CustomerServiceCassandraConf.class) {
        if (instance == null) {
          instance = new CustomerServiceCassandraConf();
        }
      }
    }
    return instance;
  }

  protected CustomerServiceCassandraConf()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerServiceTableName();
  }
}
