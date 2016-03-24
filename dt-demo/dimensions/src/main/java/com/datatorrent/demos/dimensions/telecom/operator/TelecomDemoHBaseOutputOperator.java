/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hbase.AbstractHBasePutOutputOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

public abstract class TelecomDemoHBaseOutputOperator<T> extends AbstractHBasePutOutputOperator<T> {
  
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomDemoHBaseOutputOperator.class);
  
  protected DataWarehouseConfig hbaseConfig;
  
  private boolean startOver = false;
  
  protected void configure()
  {
    //store
    HBaseStore store = new HBaseStore();
    store.setTableName(hbaseConfig.getTableName());
    store.setZookeeperQuorum(hbaseConfig.getHost());
    store.setZookeeperClientPort(hbaseConfig.getPort());

    setStore(store);
  }
  
  private boolean initialized = false;
  public void initialize()
  {
    //create table;
    try
    {
      configure();
      //The connect() in fact start a thread to connect to hbase instead of block operation.
      //it would have some time gap between return of connect() and HBase really get connected.
      getStore().connect();
      Thread.sleep(100);
      createTable();
      
      initialized = true;
    }
    catch(Exception e)
    {
      logger.error("create table '{}' failed.\n exception: {}", hbaseConfig.getTableName(), e.getMessage());
    }
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    if(!initialized)
      initialize();
  }
  

  protected void createTable() throws Exception
  {
    HBaseAdmin admin = null;
    try
    {
      admin = new HBaseAdmin(store.getConfiguration());
      final String tableName = store.getTableName();
      
      boolean hasTable = admin.isTableAvailable(tableName);
          
      if(hasTable && startOver)
      {
        admin.disableTable(tableName);
        admin.deleteTable( tableName );
        hasTable = false;
      }
      if(!hasTable)
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
      
    }
    catch (Exception e)
    {
      logger.error("exception", e);
      throw e;
    }
    finally
    {
      if (admin != null)
      {
        try
        {
          admin.close();
        }
        catch (Exception e)
        {
          logger.warn("close admin exception. ", e);
        }
      }
    }
   
  }


  public DataWarehouseConfig getHbaseConfig() {
    return hbaseConfig;
  }


  public void setHbaseConfig(DataWarehouseConfig hbaseConfig) {
    this.hbaseConfig = hbaseConfig;
  }


  public boolean isStartOver() {
    return startOver;
  }


  public void setStartOver(boolean startOver) {
    this.startOver = startOver;
  }
  
}
