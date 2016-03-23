/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCustomerServiceHiveConfig extends DataWarehouseConfig{
  private static EnrichedCustomerServiceHiveConfig instance;
  
  public static EnrichedCustomerServiceHiveConfig instance()
  {
    if(instance == null)
    {
      synchronized(EnrichedCustomerServiceHiveConfig.class)
      {
        if(instance == null)
          instance = new EnrichedCustomerServiceHiveConfig();
      }
    }
    return instance;
  }
      
  protected EnrichedCustomerServiceHiveConfig()
  {
    host = TelecomDemoConf.instance.getHiveHost();
    port = TelecomDemoConf.instance.getHivePort();
    userName = TelecomDemoConf.instance.getHiveUserName();
    password = TelecomDemoConf.instance.getHivePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getEnrichedCustomerServiceTableName();
  }
}
