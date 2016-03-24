/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.hive;

public class HiveUtil {
  public static final String driverName = "org.apache.hive.jdbc.HiveDriver";
  public static final String urlPrefix="jdbc:hive2://";
  
  public static String getUrl(String host, int port, String database)
  {
    return String.format("%s%s:%d/%s", urlPrefix, host, port, database);
  }
  
  public static void verifyDriver() throws ClassNotFoundException
  {
    Class.forName(driverName);
  }
  
}
