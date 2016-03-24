/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCDRCassandraConfig extends DataWarehouseConfig{
  private static EnrichedCDRCassandraConfig instance;
  
  public static EnrichedCDRCassandraConfig instance()
  {
    if(instance == null)
    {
      synchronized(EnrichedCDRCassandraConfig.class)
      {
        if(instance == null)
          instance = new EnrichedCDRCassandraConfig();
      }
    }
    return instance;
  }
  
  protected EnrichedCDRCassandraConfig()
  {
    host = TelecomDemoConf.instance.getCassandraHost();
    port = TelecomDemoConf.instance.getCassandraPort();
    userName = TelecomDemoConf.instance.getCassandraUserName();
    password = TelecomDemoConf.instance.getCassandraPassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }
}
