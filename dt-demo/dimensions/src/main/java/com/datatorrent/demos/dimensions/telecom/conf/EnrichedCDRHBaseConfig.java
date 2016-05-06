/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class EnrichedCDRHBaseConfig extends DataWarehouseConfig
{

  private static EnrichedCDRHBaseConfig instance;

  public static EnrichedCDRHBaseConfig instance()
  {
    if (instance == null) {
      synchronized (EnrichedCDRHBaseConfig.class) {
        if (instance == null) {
          instance = new EnrichedCDRHBaseConfig();
        }
      }
    }
    return instance;
  }

  protected EnrichedCDRHBaseConfig()
  {
    host = TelecomDemoConf.instance.getHbaseHost();
    port = TelecomDemoConf.instance.getHbasePort();
    userName = TelecomDemoConf.instance.getHbaseUserName();
    password = TelecomDemoConf.instance.getHbasePassword();
    database = TelecomDemoConf.instance.getDatabase();
    tableName = TelecomDemoConf.instance.getCdrEnrichedRecordTableName();
  }
}
