/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerEnrichedInfoCassandraConfig  extends DataWarehouseConfig{
  private static CustomerEnrichedInfoCassandraConfig instance;
  
  public static CustomerEnrichedInfoCassandraConfig instance()
  {
    if(instance == null)
    {
      synchronized(CustomerEnrichedInfoCassandraConfig.class)
      {
        if(instance == null)
          instance = new CustomerEnrichedInfoCassandraConfig();
      }
    }
    return instance;
  }
  
  protected CustomerEnrichedInfoCassandraConfig()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getCassandraPort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}
