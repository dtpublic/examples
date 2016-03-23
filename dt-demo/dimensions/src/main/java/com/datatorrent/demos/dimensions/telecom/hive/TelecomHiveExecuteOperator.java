/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.hive;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.hive.AbstractFSRollingOutputOperator.FilePartitionMapping;
import com.datatorrent.contrib.hive.HiveOperator;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

public class TelecomHiveExecuteOperator extends HiveOperator
{
  private static final Logger logger = LoggerFactory.getLogger(TelecomHiveExecuteOperator.class);
  
  protected DataWarehouseConfig hiveConfig;
  protected String createTableSql;
  protected int timeToLiveInMinutes = -1;
  protected int dataCleanupSpanInSeconds = 120;
  
  protected transient String localString = "";
  protected transient Connection conn;
  
  @Override
  public void setup(OperatorContext context)
  {
    try {
      //this class keep the url information, set to the store just to get rid of exception.
      store.setDatabaseUrl(getJdbcUrl());

      super.setup(context);

      checkIsHDFS();

      //syn tablename
      if (tablename == null || tablename.isEmpty())
        tablename = hiveConfig.getTableName();
      else
        hiveConfig.setTableName(tablename);
    } catch (IOException e) {
      logger.error("Got exception in setup.", e);
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void teardown()
  {
    closeConnection();
    super.teardown();
  }
  
  /**
   * need to remove the data
   */
  @Override
  public void endWindow()
  {
    super.endWindow();
    if(timeToLiveInMinutes >= 0)
      removeOutOfLiveData();
  }
  
  protected boolean checkIsHDFS() throws IOException
  {
    FileSystem tempFS = FileSystem.newInstance(new Path(hivestore.filepath).toUri(), new Configuration());
    if (!tempFS.getScheme().equalsIgnoreCase("hdfs")) {
      localString = " local";
      return false;
    }
    return true;
  }
  
  protected void createBusinessTables()
  {
    if(createTableSql == null || createTableSql.isEmpty())
      return;

    try {
      logger.info("creating table using sql: ");
      logger.info(createTableSql);
      Statement stmt = getConnection().createStatement(); 
      stmt.execute(createTableSql);
      logger.info("table created.");
    }
    catch (SQLException ex) {
      logger.warn("create table failed. sql is '{}'; exception: {}", createTableSql, ex.getMessage());
    }
  }
  
  @Override
  public void processTuple(FilePartitionMapping tuple)
  {
    String command = getHiveCommand(tuple);
    logger.debug("command is {}",command);
    
    //should not put comma and the end of the sql
    executeSqlCommand(command);
    
    String filePath = getFilePath(tuple);
    handleProcessedFile(filePath);
  }
  
  protected void executeSqlCommand(String sqlCommand)
  {
    if(sqlCommand == null || sqlCommand.isEmpty())
      return;
    
      Statement stmt;
      try {
        //The HiveStore has problem with user.
        stmt = getConnection().createStatement(); 
        stmt.execute(sqlCommand);
      }
      catch (SQLException ex) {
        logger.warn("execute sql command '{}' failed. reason: {}", sqlCommand, ex.getMessage());
      }
  }
  
  protected void handleProcessedFile(String filePath)
  {
    //just remove it;
    try {
      fs.delete(new Path(filePath), true);
    } catch (IllegalArgumentException | IOException e) {
      logger.error("delete file exception. ", e);
    }
  }
  
  /**
   * The HiveStore has problem with user.
   */
  protected Connection getConnection()
  {
    if(conn == null)
      try {
        conn = DriverManager.getConnection( getJdbcUrl(), hiveConfig.getUserName(), hiveConfig.getPassword());
      } catch (SQLException e) {
        logger.error("connection",e);
      }
    return conn;
  }
  
  protected void closeConnection()
  {
    if(conn != null)
    {
      try {
        conn.close();
      } catch (SQLException e) {
        logger.warn("close connection exception: " + e.getMessage());
      }
    }
  }
  
  protected String getJdbcUrl()
  {
    return HiveUtil.getUrl(hiveConfig.getHost(), hiveConfig.getPort(), hiveConfig.getDatabase());
  }
  
  protected boolean isAbsolutePath(String filePath)
  {
    return (filePath.length() > 0 && filePath.startsWith(File.separator));
  }
  
  protected String getFilePath(FilePartitionMapping tuple)
  {
    String filename = tuple.getFilename();
    ArrayList<String> partition = tuple.getPartition();
    String filepath = isAbsolutePath(filename) ? filename : hivestore.getFilepath() + Path.SEPARATOR + filename;
    logger.info("processing file '{}'.", filepath);
    return filepath;
  }
  
  protected String getHiveCommand(FilePartitionMapping tuple)
  {
    String filepath = getFilePath(tuple);
    
    ArrayList<String> partition = tuple.getPartition();
    int numPartitions = partition.size();
    
    String command = null;
    try {
      if (fs.exists(new Path(filepath))) {
        String partitionString = createPartitionString(); 
        if (partitionString != null && !partitionString.isEmpty() ) {
          command = "load data " + localString + " inpath '" + filepath + "' into table " + tablename + " PARTITION" + "( " + partitionString + " )";
        }
        else {
          command = "load data " + localString + " inpath '" + filepath + "' into table " + tablename;
        }
      }
      else
        logger.warn("Path '{}' not exists.", filepath);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.info("command is {}" , command);
    return command;
  }
  
  protected String createPartitionString()
  {
    return "createdtime=" + Calendar.getInstance().getTimeInMillis();
  }
  
  /**
   * the data was stored partitioned by 'createdtime'
   */
  protected transient long dataCleanupTime = 0;
  protected void removeOutOfLiveData()
  {
    long curTime = Calendar.getInstance().getTimeInMillis();
    if(curTime < dataCleanupTime + 1000*dataCleanupSpanInSeconds )
      return;
    
    String cleanupSql = "alter table %s drop partition(createdtime<%d)".format(tablename, curTime - timeToLiveInMinutes*60*1000);
    this.executeSqlCommand(cleanupSql);
    
    dataCleanupTime = curTime; 
  }
  

  public DataWarehouseConfig getHiveConfig()
  {
    return hiveConfig;
  }

  public void setHiveConfig(DataWarehouseConfig hiveConfig)
  {
    this.hiveConfig = hiveConfig;
  }

  public String getCreateTableSql()
  {
    return createTableSql;
  }

  public void setCreateTableSql(String createTableSql)
  {
    this.createTableSql = createTableSql;
  }

  public int getTimeToLiveHour()
  {
    return timeToLiveInMinutes;
  }

  public void setTimeToLiveHour(int timeToLiveHour)
  {
    this.timeToLiveInMinutes = timeToLiveHour;
  }
  
}
