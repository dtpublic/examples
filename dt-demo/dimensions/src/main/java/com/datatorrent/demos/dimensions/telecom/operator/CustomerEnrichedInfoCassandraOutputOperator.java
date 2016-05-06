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

import com.datatorrent.demos.dimensions.telecom.conf.CustomerEnrichedInfoCassandraConfig;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoCassandraOutputOperator extends TelecomDemoCassandraOutputOperator<SingleRecord>
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerEnrichedInfoCassandraOutputOperator.class);

  public CustomerEnrichedInfoCassandraOutputOperator()
  {
    cassandraConfig = CustomerEnrichedInfoCassandraConfig.instance();
  }

  @Override
  protected void createBusinessTables(Session session)
  {
    String createTable = "CREATE TABLE IF NOT EXISTS " + cassandraConfig.getDatabase() + "."
        + cassandraConfig.getTableName()
        + " (id text PRIMARY KEY, imsi text, isdn text, imei text, operatorName text, operatorCode text, deviceBrand text, deviceModel text);";
    session.execute(createTable);
    logger.info("created table: {}", cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName());
  }

  protected String createSqlFormat()
  {
    sqlCommand = "INSERT INTO " + cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName()
        + " ( id, imsi, isdn, imei, operatorName, operatorCode, deviceBrand, deviceModel ) "
        + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ? );";
    return sqlCommand;
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, SingleRecord tuple) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    boundStmnt.setString(0, tuple.id);
    boundStmnt.setString(1, tuple.imsi);
    boundStmnt.setString(2, tuple.isdn);
    boundStmnt.setString(3, tuple.imei);
    boundStmnt.setString(4, tuple.operatorName);
    boundStmnt.setString(5, tuple.operatorCode);
    boundStmnt.setString(6, tuple.deviceBrand);
    boundStmnt.setString(7, tuple.deviceModel);

    //or boundStatement.bind();
    return boundStmnt;
  }

}
