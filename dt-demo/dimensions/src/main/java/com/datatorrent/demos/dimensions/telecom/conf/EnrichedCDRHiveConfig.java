/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCDRHiveConfig extends DataWarehouseConfig
{
  private static EnrichedCDRHiveConfig instance;

  public static EnrichedCDRHiveConfig instance()
  {
    if (instance == null) {
      synchronized (EnrichedCDRHiveConfig.class) {
        if (instance == null)
          instance = new EnrichedCDRHiveConfig();
      }
    }
    return instance;
  }

  protected EnrichedCDRHiveConfig()
  {
    host = TelecomDemoConf.instance.getHiveHost();
    port = TelecomDemoConf.instance.getHivePort();
    userName = TelecomDemoConf.instance.getHiveUserName();
    password = TelecomDemoConf.instance.getHivePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }

}
