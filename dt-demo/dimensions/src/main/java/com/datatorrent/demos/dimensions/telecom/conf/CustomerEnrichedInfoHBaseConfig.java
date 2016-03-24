/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class CustomerEnrichedInfoHBaseConfig  extends DataWarehouseConfig{
  private static CustomerEnrichedInfoHBaseConfig instance;
  
  public static CustomerEnrichedInfoHBaseConfig instance()
  {
    if(instance == null)
    {
      synchronized(CustomerEnrichedInfoHBaseConfig.class)
      {
        if(instance == null)
          instance = new CustomerEnrichedInfoHBaseConfig();
      }
    }
    return instance;
  }
  
  protected CustomerEnrichedInfoHBaseConfig()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCustomerEnrichedInfoTableName();
  }
}
