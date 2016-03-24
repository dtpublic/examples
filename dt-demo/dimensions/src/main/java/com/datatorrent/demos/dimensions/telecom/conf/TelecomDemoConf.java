/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.conf;

/**
 * hard code the configuration here.
 * it can be loaded from configuration file later.
 * 
 * @author bright
 *
 */
public class TelecomDemoConf {
  public static TelecomDemoConf instance = new TelecomDemoConf();
  
  //for Hive
  protected String hiveHost = "node22";
  protected int hivePort = 10000;
  protected String hiveUserName = "bright";
  protected String hivePassword = "";

  //for HBase
  protected String hbaseHost = "node26";
  protected int hbasePort = 2181;
  protected String hbaseUserName = "bright";
  protected String hbasePassword = "";
  
  //cassandra
  protected String cassandraHost = "node22";
  protected int cassandraPort = 9160;
  protected String cassandraUserName = "bright";
  protected String cassandraPassword = "";
  
  protected String database = "telecomdemo";
  protected String customerEnrichedInfoTableName = "CustomerEnrichedInfo";
  protected String cdrEnrichedRecordTableName = "CDREnrichedRecord";
  
  protected String cdrDir = "CDR";
  
  protected String customerServiceTableName = "CustomerService";
  protected String EnrichedCustomerServiceTableName = "EnrichedCustomerService";
  
  private TelecomDemoConf(){}
  
  public String getHiveHost() {
    return hiveHost;
  }
  public void setHiveHost(String hiveHost) {
    this.hiveHost = hiveHost;
  }
  public int getHivePort() {
    return hivePort;
  }
  public void setHivePort(int hivePort) {
    this.hivePort = hivePort;
  }
  public String getDatabase() {
    return database;
  }
  public void setDatabase(String database) {
    this.database = database;
  }
  public String getCustomerEnrichedInfoTableName() {
    return customerEnrichedInfoTableName;
  }
  public void setCustomerEnrichedInfoTableName(String customerEnrichedInfoTableName) {
    this.customerEnrichedInfoTableName = customerEnrichedInfoTableName;
  }
  public String getHiveUserName() {
    return hiveUserName;
  }
  public void setHiveUserName(String hiveUserName) {
    this.hiveUserName = hiveUserName;
  }
  public String getHivePassword() {
    return hivePassword;
  }
  public void setHivePassword(String hivePassword) {
    this.hivePassword = hivePassword;
  }

  public String getHbaseHost() {
    return hbaseHost;
  }

  public void setHbaseHost(String hbaseHost) {
    this.hbaseHost = hbaseHost;
  }

  public int getHbasePort() {
    return hbasePort;
  }

  public void setHbasePort(int hbasePort) {
    this.hbasePort = hbasePort;
  }

  public String getHbaseUserName() {
    return hbaseUserName;
  }

  public void setHbaseUserName(String hbaseUserName) {
    this.hbaseUserName = hbaseUserName;
  }

  public String getHbasePassword() {
    return hbasePassword;
  }

  public void setHbasePassword(String hbasePassword) {
    this.hbasePassword = hbasePassword;
  }

  public String getCdrEnrichedRecordTableName() {
    return cdrEnrichedRecordTableName;
  }

  public void setCdrEnrichedRecordTableName(String cdrEnrichedRecordTableName) {
    this.cdrEnrichedRecordTableName = cdrEnrichedRecordTableName;
  }

  public String getCdrDir() {
    return cdrDir;
  }

  public void setCdrDir(String cdrDir) {
    this.cdrDir = cdrDir;
  }

  public String getCustomerServiceTableName() {
    return customerServiceTableName;
  }

  public void setCustomerServiceTableName(String customerServiceTableName) {
    this.customerServiceTableName = customerServiceTableName;
  }

  public String getCassandraHost()
  {
    return cassandraHost;
  }

  public void setCassandraHost(String cassandraHost)
  {
    this.cassandraHost = cassandraHost;
  }

  public int getCassandraPort()
  {
    return cassandraPort;
  }

  public void setCassandraPort(int cassandraPort)
  {
    this.cassandraPort = cassandraPort;
  }

  public String getCassandraUserName()
  {
    return cassandraUserName;
  }

  public void setCassandraUserName(String cassandraUserName)
  {
    this.cassandraUserName = cassandraUserName;
  }

  public String getCassandraPassword()
  {
    return cassandraPassword;
  }

  public void setCassandraPassword(String cassandraPassword)
  {
    this.cassandraPassword = cassandraPassword;
  }

  public String getEnrichedCustomerServiceTableName()
  {
    return EnrichedCustomerServiceTableName;
  }

  public void setEnrichedCustomerServiceTableName(String enrichedCustomerServiceTableName)
  {
    EnrichedCustomerServiceTableName = enrichedCustomerServiceTableName;
  }
  
  
}
