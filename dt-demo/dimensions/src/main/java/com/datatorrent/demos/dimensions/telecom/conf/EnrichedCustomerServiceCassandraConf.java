/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCustomerServiceCassandraConf extends DataWarehouseConfig
{
  private static EnrichedCustomerServiceCassandraConf instance;

  public static EnrichedCustomerServiceCassandraConf instance()
  {
    if (instance == null) {
      synchronized (EnrichedCustomerServiceCassandraConf.class) {
        if (instance == null) {
          instance = new EnrichedCustomerServiceCassandraConf();
        }
      }
    }
    return instance;
  }

  protected EnrichedCustomerServiceCassandraConf()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getEnrichedCustomerServiceTableName();
  }
}
