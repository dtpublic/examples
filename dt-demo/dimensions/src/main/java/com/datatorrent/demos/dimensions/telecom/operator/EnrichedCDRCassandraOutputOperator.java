/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCDRCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.generate.GeneratorUtil;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;

public class EnrichedCDRCassandraOutputOperator extends TelecomDemoCassandraOutputOperator<EnrichedCDR>
{
  private static final transient Logger logger = LoggerFactory.getLogger(EnrichedCDRCassandraOutputOperator.class);
  private int ttl = 24*60*60;   //default one day
  
  public EnrichedCDRCassandraOutputOperator()
  {
    cassandraConfig = EnrichedCDRCassandraConfig.instance();
  }
  
  @Override
  protected void createBusinessTables(Session session)
  {
    String createTable = "CREATE TABLE IF NOT EXISTS " + cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName()
        + " (id bigint PRIMARY KEY, imsi text, isdn text, imei text, plan text, callType text, correspType text, correspIsdn text, duration int, "
        + "bytes int, dr int, lat float, lon float, date text, time text, drLabel text, operatorCode text, deviceBrand text, "
        + "deviceModel text, zipcode text );";
    session.execute(createTable);
   
  }
  
  protected String createSqlFormat()
  {
    sqlCommand = "INSERT INTO " + cassandraConfig.getDatabase() + "."
        + cassandraConfig.getTableName()
        + " ( id, imsi, isdn, imei, plan, callType, correspType, correspIsdn, duration, bytes, dr, lat, lon, date, time, drLabel, operatorCode, deviceBrand, deviceModel, zipcode ) "
        + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ) USING TTL " + ttl + ";";
    return sqlCommand;
  }
  

  private long id = GeneratorUtil.getRecordId();
  
  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, EnrichedCDR tuple) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    int index = 0;
    boundStmnt.setLong(index++, ++id);
    boundStmnt.setString(index++, tuple.getImsi());
    boundStmnt.setString(index++, tuple.getIsdn());
    boundStmnt.setString(index++, tuple.getImei());
    boundStmnt.setString(index++, tuple.getPlan());
    boundStmnt.setString(index++, tuple.getCallType());
    boundStmnt.setString(index++, tuple.getCallType());
    boundStmnt.setString(index++, tuple.getCorrespType());
    boundStmnt.setInt(index++, tuple.getDuration());
    boundStmnt.setInt(index++, tuple.getBytes());
    boundStmnt.setInt(index++, tuple.getDr());
    boundStmnt.setFloat(index++, tuple.getLat());
    boundStmnt.setFloat(index++, tuple.getLon());
    boundStmnt.setString(index++, tuple.getDate());
    boundStmnt.setString(index++, tuple.getTimeInDay());
    boundStmnt.setString(index++, tuple.getDrLabel());
    boundStmnt.setString(index++, tuple.getOperatorCode());
    boundStmnt.setString(index++, tuple.getDeviceBrand());
    boundStmnt.setString(index++, tuple.getDeviceModel());
    boundStmnt.setString(index++, tuple.getZipCode());
    
    //or boundStatement.bind();
    return boundStmnt;
  }

  public int getTtl()
  {
    return ttl;
  }
  public void setTtl(int ttl)
  {
    this.ttl = ttl;
  }
  
}