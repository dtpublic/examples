/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHiveConfig;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.hive.HiveUtil;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoHiveOutputOperator extends BaseOperator{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoHiveOutputOperator.class);
  
  private DataWarehouseConfig hiveConfig = CustomerEnrichedInfoHiveConfig.instance();
  
  private boolean startOver = false;
  
  protected Connection connect;
  
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<SingleRecord> input = new DefaultInputPort<SingleRecord>()
  {
    @Override
    public void process(SingleRecord t)
    {
      processTuple(t);
    }
  };
  
  @Override
  public void setup(OperatorContext context)
  {
    //create table;
    try
    {
      createTable();
    }
    catch(Exception e)
    {
      logger.error("create table '{}' failed.\n exception: {}", hiveConfig.getTableName(), e.getMessage());
    }
  }
  
  protected Connection getConnect() throws SQLException, ClassNotFoundException
  {
    HiveUtil.verifyDriver();
    
    if(connect == null)
    {
      String url = HiveUtil.getUrl(hiveConfig.getHost(), hiveConfig.getPort(), hiveConfig.getDatabase());
      connect = DriverManager.getConnection(url, hiveConfig.getUserName(), hiveConfig.getPassword());    
    }
    return connect;
  }

  protected void createTable() throws Exception
  {
    Connection connect = getConnect();
    Statement stmt = connect.createStatement();
    
    ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + hiveConfig.getTableName() + "'");
    //hive doesn't support first();
    //boolean hasTable = rs.first();
    boolean hasTable = false;
    try
    {
      hasTable = rs.next();
    }
    catch(SQLException e)
    {
      logger.warn(e.getMessage());
    }
    
    if(hasTable && startOver)
    {
      stmt.executeUpdate("drop table " + hiveConfig.getTableName());
      hasTable = false;
    }
    
    if(!hasTable)
    {
      //create table;
      //not support long?
      String tableSchema = " (id String, imsi string, isdn string, imei string, operatorCode string, operatorName string, deviceBrand string, deviceModel string)";
      stmt.executeUpdate("create table " + hiveConfig.getTableName() + tableSchema);
    }
    stmt.close();
  }
  
  private Statement insertStatement;
  protected Statement getInsertStatement() throws ClassNotFoundException, SQLException
  {
    if(insertStatement == null)
      insertStatement = getConnect().createStatement();
    return insertStatement;
  }

  private int batchCount = 1000;
  private int batchSize = 0;
  public void processTuple(SingleRecord tuple)
  {
    final String sqlValueFormat = "('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
    String sql = "insert into table " + hiveConfig.getTableName() + " values " 
        + String.format( sqlValueFormat, tuple.id, tuple.imsi, tuple.isdn, tuple.imei, tuple.operatorCode, tuple.operatorName, tuple.deviceBrand, tuple.deviceModel );
    
    try {
      //hive doesn't support batch
      //getInsertStatement().addBatch(sql);
      getInsertStatement().executeUpdate(sql);
//      ++batchSize;
//      
//      if(batchSize >= batchCount)
//      {
//        insertStatement.executeBatch();
//        batchSize = 0;
//      }
      
    } catch (ClassNotFoundException | SQLException e) {
      logger.error(e.getMessage(), e);
    }
  }
  
  @Override
  public void endWindow()
  {
//    try
//    {
//      if(batchSize > 0)
//        insertStatement.executeBatch();
//    }
//    catch(SQLException e)
//    {
//      logger.error(e.getMessage(), e);
//    }
  }

  public DataWarehouseConfig getHiveConfig() {
    return hiveConfig;
  }

  public void setHiveConfig(DataWarehouseConfig hiveConfig) {
    this.hiveConfig = hiveConfig;
  }
  
  
}
