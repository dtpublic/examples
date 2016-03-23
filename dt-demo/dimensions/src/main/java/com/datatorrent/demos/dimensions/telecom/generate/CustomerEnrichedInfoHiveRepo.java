/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHiveConfig;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.hive.HiveUtil;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;
import com.google.common.collect.Lists;

public class CustomerEnrichedInfoHiveRepo {
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoHbaseRepo.class);
  
  private SingleRecord[] customerInfoArray;
  
  private DataWarehouseConfig hiveConfig = CustomerEnrichedInfoHiveConfig.instance();

  protected Connection connect;
  
  protected Random random = new Random();

  private static CustomerEnrichedInfoHiveRepo instance;
  
  public static CustomerEnrichedInfoHiveRepo getInstance()
  {
    return instance;
  }
  public static CustomerEnrichedInfoHiveRepo createInstance(DataWarehouseConfig conf)
  {
    if(instance == null)
    {
      synchronized(CustomerEnrichedInfoHbaseRepo.class)
      {
        if(instance == null)
        {
          instance = new CustomerEnrichedInfoHiveRepo();
          instance.setHiveConfig(conf);
          try
          {
            instance.load();
          }
          catch(Exception e)
          {
            logger.error("Can't load CustomerEnrichedInfo. check config: {}. exception: {}.", conf, e.getMessage());
            instance = null;
          }
        }
      }
    }
    else
    {
      if(!instance.hiveConfig.equals(conf))
      {
        throw new IllegalArgumentException("CustomerEnrichedInfoRepo suppose only load from same datasourc.");
      }
    }
    return instance;
  }
  
  private CustomerEnrichedInfoHiveRepo()
  {
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
  
  protected void load() throws ClassNotFoundException, SQLException
  {
    Statement queryStmt = getConnect().createStatement();
    //id long, imsi string, isdn string, imei string, operatorCode string, operatorName string, deviceBrand string, deviceModel string
    String sql = "select id, imsi, isdn, imei, operatorCode, operatorName, deviceBrand, deviceModel from " + hiveConfig.getTableName();
    ResultSet rs = queryStmt.executeQuery(sql);
    
    if( !rs.first() )
    {
      logger.warn("No Record for customer enriched info.");
      rs.close();
      return;
    }
    
    List<SingleRecord> customerInfoList = Lists.newArrayList();
    while( rs.next() )
    {
      int columnIndex = 0;
      String id = rs.getString(columnIndex++);
      String imsi = rs.getString(columnIndex++);
      String isdn = rs.getString(columnIndex++);
      String imei = rs.getString(columnIndex++);
      String operatorCode = rs.getString(columnIndex++);
      String operatorName = rs.getString(columnIndex++);
      String deviceBrand = rs.getString(columnIndex++);
      String deviceModel = rs.getString(columnIndex++);
      
      SingleRecord record = new SingleRecord(id, imsi, isdn, imei, operatorCode, operatorName, deviceBrand, deviceModel);
      customerInfoList.add(record);
      
    }
    
    customerInfoArray = customerInfoList.toArray(new SingleRecord[0]);
  }
  
  public SingleRecord getRandomCustomerEnrichedInfo()
  {
    int index = random.nextInt(customerInfoArray.length);
    return customerInfoArray[index];
  }
  
  
  public DataWarehouseConfig getHiveConfig() {
    return hiveConfig;
  }
  public void setHiveConfig(DataWarehouseConfig hiveConfig) {
    this.hiveConfig = hiveConfig;
  }
}
