/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

public class DataWarehouseConfig
{
  protected String host;
  protected int port;

  protected String userName;
  protected String password;

  protected String database;
  protected String tableName;

  public String getHost()
  {
    return host;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassword()
  {
    return password;
  }

  public void setPassword(String password)
  {
    this.password = password;
  }

  public String getDatabase()
  {
    return database;
  }

  public void setDatabase(String database)
  {
    this.database = database;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  @Override
  public String toString()
  {
    return String.format("host=%s; port=%d; userName=%s; database=%s; tableName=%s ", host, port, userName, database,
        tableName);
  }
}
