/**
 * Copyright (c) 2016 DataTorrent, Inc.
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

import com.datatorrent.demos.dimensions.telecom.conf.CustomerServiceCassandraConf;
import com.datatorrent.demos.dimensions.telecom.generate.GeneratorUtil;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;

public class CustomerServiceCassandraOutputOperator extends TelecomDemoCassandraOutputOperator<CustomerService>
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceCassandraOutputOperator.class);

  public CustomerServiceCassandraOutputOperator()
  {
    cassandraConfig = CustomerServiceCassandraConf.instance();
  }

  @Override
  protected void createBusinessTables(Session session)
  {

    String createTable = "CREATE TABLE IF NOT EXISTS " + cassandraConfig.getDatabase() + "."
        + cassandraConfig.getTableName()
        + " (id bigint PRIMARY KEY, imsi text, totalDuration int, wait int, zipCode text, issueType text, satisfied boolean);";
    session.execute(createTable);

  }

  protected String createSqlFormat()
  {
    sqlCommand = "INSERT INTO " + cassandraConfig.getDatabase() + "." + cassandraConfig.getTableName()
        + " ( id, imsi, totalDuration, wait, zipCode, issueType, satisfied ) " + "VALUES ( ?, ?, ?, ?, ?, ?, ?, ? );";
    return sqlCommand;
  }

  private long id = GeneratorUtil.getRecordId();

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, CustomerService tuple)
      throws DriverException
  {
    final BoundStatement boundStmnt = new BoundStatement(updateCommand);
    boundStmnt.setLong(0, ++id);
    boundStmnt.setString(1, tuple.imsi);
    boundStmnt.setInt(2, tuple.totalDuration);
    boundStmnt.setInt(3, tuple.wait);
    boundStmnt.setString(4, tuple.zipCode);
    boundStmnt.setString(5, tuple.issueType.name());
    boundStmnt.setBool(6, tuple.satisfied);

    //or boundStatement.bind();
    return boundStmnt;

  }
}
