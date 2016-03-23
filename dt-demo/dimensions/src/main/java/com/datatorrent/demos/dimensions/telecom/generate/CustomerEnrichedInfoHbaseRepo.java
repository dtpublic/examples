/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoHBaseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;
import com.google.common.collect.Lists;

/**
 * This class load Customer Enriched information and keep in memory
 * 
 * @author bright
 *
 */
public class CustomerEnrichedInfoHbaseRepo implements CustomerEnrichedInfoProvider{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoHbaseRepo.class);
  
  private SingleRecord[] customerInfoArray;
  
  private DataWarehouseConfig hbaseConfig = CustomerEnrichedInfoHBaseConfig.instance();

  
  protected Random random = new Random();

  private static CustomerEnrichedInfoHbaseRepo instance;
  
  protected HBaseStore store;
  
  public static CustomerEnrichedInfoHbaseRepo getInstance()
  {
    return instance;
  }
  public static CustomerEnrichedInfoHbaseRepo createInstance(DataWarehouseConfig conf)
  {
    if(instance == null)
    {
      synchronized(CustomerEnrichedInfoHbaseRepo.class)
      {
        if(instance == null)
        {
          instance = new CustomerEnrichedInfoHbaseRepo();
          instance.setHbaseConfig(conf);
          
          try
          {
            instance.initialize();
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
      if(!instance.hbaseConfig.equals(conf))
      {
        throw new IllegalArgumentException("CustomerEnrichedInfoRepo suppose only load from same datasourc.");
      }
    }
    return instance;
  }
  
  private CustomerEnrichedInfoHbaseRepo()
  {
  }
  
  protected void initialize() throws IOException
  {
    //store
    store = new HBaseStore();
    store.setTableName(hbaseConfig.getTableName());
    store.setZookeeperQuorum(hbaseConfig.getHost());
    store.setZookeeperClientPort(hbaseConfig.getPort());

    store.connect();
    
    store.getTable();
    
  }
  
  protected void load() throws ClassNotFoundException, SQLException
  {
    List<SingleRecord> customerInfoList = Lists.newArrayList();
    try {
      HTable table = store.getTable();
      Scan scan = new Scan();
      ResultScanner scanner = table.getScanner(scan);
      
      Map<String,String> nameValueMap = new HashMap<String,String>();
      while( true )
      {
        Result result = scanner.next();
        if( result == null )
          break;
        
        nameValueMap.clear();
        
        String imsi = Bytes.toString( result.getRow() );
        nameValueMap.put("imsi", imsi);
        
        List<Cell> cells = result.listCells();
        for( Cell cell : cells )
        {
          String columnName = Bytes.toString( CellUtil.cloneQualifier(cell) );
          String value = Bytes.toString( CellUtil.cloneValue(cell) );
          nameValueMap.put(columnName, value);
        }
        SingleRecord record = new SingleRecord(nameValueMap);
        
        customerInfoList.add(record);
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
    
    customerInfoArray = customerInfoList.toArray(new SingleRecord[0]);
  }
  
  @Override
  public SingleRecord getRandomCustomerEnrichedInfo()
  {
    int index = random.nextInt(customerInfoArray.length);
    return customerInfoArray[index];
  }
  public DataWarehouseConfig getHbaseConfig()
  {
    return hbaseConfig;
  }
  public void setHbaseConfig(DataWarehouseConfig hbaseConfig)
  {
    this.hbaseConfig = hbaseConfig;
  }
  
}
