/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;

public abstract class TelecomDemoCassandraOutputOperator<T> extends BaseOperator
{
  private static final transient Logger logger = LoggerFactory.getLogger(TelecomDemoCassandraOutputOperator.class);

  public final transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  protected DataWarehouseConfig cassandraConfig;
  protected String sqlCommand;

  protected int batchSize = 100;
  protected transient Session session;

  protected transient PreparedStatement preparedStatement;
  private transient BatchStatement batchStatement;

  @Override
  public void setup(OperatorContext context)
  {
    logger.info("setup() starting");
    configure();
    createSession();
    createTables();
    createSqlFormat();

    preparedStatement = prepareStatement();
    logger.info("setup() done.");
  }

  @Override
  public void teardown()
  {
    closeSession();
    super.teardown();
  }

  protected abstract String createSqlFormat();

  protected void configure()
  {
  }

  protected void createSession()
  {
    Cluster cluster = Cluster.builder().addContactPoint(cassandraConfig.getHost()).build();
    session = cluster.connect(cassandraConfig.getDatabase());
  }

  protected void closeSession()
  {
    if (session != null) {
      session.close();
    }
  }

  protected void createTables()
  {
    createBusinessTables(session);
  }

  protected abstract void createBusinessTables(Session session);

  //@Override
  protected PreparedStatement prepareStatement()
  {
    return session.prepare(sqlCommand);
  }

  protected abstract Statement setStatementParameters(PreparedStatement updateCommand, T tuple) throws DriverException;

  public void processTuple(T tuple)
  {
    if (batchStatement == null) {
      batchStatement = new BatchStatement();
    }
    
    batchStatement.add(setStatementParameters(preparedStatement, tuple));

    if (batchStatement.size() >= batchSize) {
      session.execute(batchStatement);
      batchStatement.clear();
    }
  }

  public DataWarehouseConfig getCassandraConfig()
  {
    return cassandraConfig;
  }

  public void setCassandraConfig(DataWarehouseConfig cassandraConfig)
  {
    this.cassandraConfig = cassandraConfig;
  }

}
