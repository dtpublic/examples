/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.contrib.cassandra.CassandraPOJOOutputOperator;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;

/**
 * Extends {@link CassandraPOJOOutputOperator} to write tuples to cassandra database.<br/>
 * This operator keeps track of hashcode of all written data tuples and emits
 * ScanInformation which can be used for data validation.<br/>
 * Operator Ports:<br/>
 * 1. tuplesInput: Receives pojo tuples<br/>
 * 2. ValidationInfo: emits {@link ScanInformation} required for data validation
 * by downstream operators
 *
 */
public class CassandraDataPopulator extends CassandraPOJOOutputOperator
{
  private int numRowsToWrite;
  private int processedTuples;
  private int hashCode;
  private boolean writeMoreRecords = true;

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<ScanInformation> validatationInfo = new DefaultOutputPort<>();

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<TestUsers> tuplesInput = new DefaultInputPort<TestUsers>()
  {
    @Override
    public void setup(Context.PortContext context)
    {
      pojoClass = context.getValue(Context.PortContext.TUPLE_CLASS);
    }

    @Override
    public void process(TestUsers tuple)
    {
      if (writeMoreRecords) {
        CassandraDataPopulator.super.input.process(tuple);
        hashCode = hashCode ^ tuple.hashCode();
        processedTuples++;

        if (processedTuples >= numRowsToWrite) {
          //emit scan info, this will also trigger scan in jdbc input operator
          validatationInfo.emit(new ScanInformation(numRowsToWrite, hashCode));
          writeMoreRecords = false;
        }
      }
    }

  };

  @Override
  public void setup(OperatorContext context)
  {
    Cluster cluster = Cluster.builder().addContactPoint(store.getNode()).build();
    Session session = cluster.connect(store.getKeyspace());
    String createMetaTable = "CREATE TABLE IF NOT EXISTS " + store.getKeyspace() + "."
        + CassandraTransactionalStore.DEFAULT_META_TABLE + " ( " + CassandraTransactionalStore.DEFAULT_APP_ID_COL
        + " TEXT, " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL + " INT, "
        + CassandraTransactionalStore.DEFAULT_WINDOW_COL + " BIGINT, " + "PRIMARY KEY ("
        + CassandraTransactionalStore.DEFAULT_APP_ID_COL + ", " + CassandraTransactionalStore.DEFAULT_OPERATOR_ID_COL
        + ") " + ");";
    session.execute(createMetaTable);
    String createTable = "CREATE TABLE IF NOT EXISTS " + store.getKeyspace() + "." + getTablename()
        + " (id uuid PRIMARY KEY,fname text,lname text,city text);";
    session.execute(createTable);
    session.close();
    cluster.close();

    super.setup(context);
  }

  @Override
  protected Statement setStatementParameters(PreparedStatement updateCommand, Object tuple) throws DriverException
  {
    Statement stmt = super.setStatementParameters(updateCommand, tuple);
    stmt.setConsistencyLevel(ConsistencyLevel.ALL);
    return stmt;
  }

  /**
   * Get total number of rows to be written, requires for validation
   * 
   * @return numRowsToWrite
   */
  public int getNumRowsToWrite()
  {
    return numRowsToWrite;
  }

  /**
   * Set total number of rows to be written, requires for validation
   * 
   * @param numRowsToWrite
   */
  public void setNumRowsToWrite(int numRowsToWrite)
  {
    this.numRowsToWrite = numRowsToWrite;
  }
}
