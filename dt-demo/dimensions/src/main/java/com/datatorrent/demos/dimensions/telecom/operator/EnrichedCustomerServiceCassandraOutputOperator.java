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

import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCustomerServiceCassandraConf;
import com.datatorrent.demos.dimensions.telecom.generate.GeneratorUtil;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;

public class EnrichedCustomerServiceCassandraOutputOperator extends TelecomDemoCassandraOutputOperator<EnrichedCustomerService>
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceCassandraOutputOperator.class);

  public EnrichedCustomerServiceCassandraOutputOperator()
  {
    cassandraConfig = EnrichedCustomerServiceCassandraConf.instance();
  }
  
  @Override
  protected void createBusinessTables(Session session)
  {
    
    String createTable = "CREATE TABLE IF NOT EXISTS " + cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName()
        + " (id bigint PRIMARY KEY, imsi text, totalDuration int, wait int, zipCode text, issueType text, satisfied boolean, "
        + " operatorCode text, deviceBrand text, deviceModel text)";
    session.execute(createTable);
  }
  
  protected String createSqlFormat()
  {
    sqlCommand = "INSERT INTO " + cassandraConfig.getDatabase() + "."
        + cassandraConfig.getTableName()
        + " ( id, imsi, totalDuration, wait, zipCode, issueType, satisfied, operatorCode, deviceBrand, deviceModel ) "
        + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
    return sqlCommand;
  }
  

  private long id = GeneratorUtil.getRecordId();
  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, EnrichedCustomerService tuple) throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    boundStmnt.setLong(0, ++id);
    boundStmnt.setString(1, tuple.imsi);
    boundStmnt.setInt(2, tuple.totalDuration);
    boundStmnt.setInt(3, tuple.wait);
    boundStmnt.setString(4, tuple.zipCode);
    boundStmnt.setString(5, tuple.issueType.name());
    boundStmnt.setBool(6, tuple.satisfied);
    boundStmnt.setString(7, tuple.operatorCode);
    boundStmnt.setString(8, tuple.deviceBrand);
    boundStmnt.setString(9, tuple.deviceModel);
    
    //or boundStatement.bind();
    return boundStmnt;
    
  }

}
