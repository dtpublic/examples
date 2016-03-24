/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerEnrichedInfoHiveConfig extends DataWarehouseConfig{
  private static CustomerEnrichedInfoHiveConfig instance;
  
  public static CustomerEnrichedInfoHiveConfig instance()
  {
    if(instance == null)
    {
      synchronized(CustomerEnrichedInfoHiveConfig.class)
      {
        if(instance == null)
          instance = new CustomerEnrichedInfoHiveConfig();
      }
    }
    return instance;
  }
  
  
  protected CustomerEnrichedInfoHiveConfig()
  {
    host = TelecomDemoConf.instance.getHiveHost();
    port = TelecomDemoConf.instance.getHivePort();
    userName = TelecomDemoConf.instance.getHiveUserName();
    password = TelecomDemoConf.instance.getHivePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}
