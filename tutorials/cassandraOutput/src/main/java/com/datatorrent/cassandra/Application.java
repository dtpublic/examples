/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.cassandra.CassandraPOJOOutputOperator;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.contrib.util.POJOTupleGenerateOperator;

/**
 * Application to test Cassandra Output Operator from Apache Apex-Malhar library
 * Application has following operators:<br/>
 * 1. TuplesDataGenerator: Generates configured number of tupes for testing (default: 5000).<br/>
 * 2. CassandraPOJOOutputOperator (from Malhar): Writes data to cassandra DB.<br/>
 *
 * @author priyanka
 *
 */
@ApplicationAnnotation(name = "CassandraOutputApplication")
public class Application implements StreamingApplication
{

  private static final int DEFAULT_WRITE_COUNT = 5000;

  public void populateDAG(DAG dag, Configuration conf)
  {
    int tuplesCount = conf.getInt("dt.application.CassandraOutputApplication.prop.rowsCount", DEFAULT_WRITE_COUNT);

    PojoGenerator tuplesGenerator = new PojoGenerator();
    tuplesGenerator.setTupleNum(tuplesCount);

    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();

    CassandraPOJOOutputOperator cassandraOutput = new CassandraPOJOOutputOperator();
    cassandraOutput.setStore(transactionalStore);

    dag.addOperator("TuplesDataGenerator", tuplesGenerator);
    dag.addOperator("CassandraDataWriter", cassandraOutput);
    dag.addStream("tuplesToDatabase", tuplesGenerator.outputPort, cassandraOutput.input);
  }
}

/**
 * Generates Test pojo tuples.
 *
 */
class PojoGenerator extends POJOTupleGenerateOperator<TestUser>
{
  private static int counter = 0;
  // cassandra output operator can't handle batch sizes bigger than 50kb (as
  // per default configuration)
  private int batchSizePerWindow = 100;
  private int tuplesEmittedInWindow;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    tuplesEmittedInWindow = 0;
  }

  @Override
  public void emitTuples()
  {
    if (tuplesEmittedInWindow < batchSizePerWindow) {
      int tuplesCountBeforeEmit = super.getEmitedTupleCount();
      super.emitTuples();
      int tuplesCountAfterEmit = super.getEmitedTupleCount();
      tuplesEmittedInWindow += (tuplesCountAfterEmit - tuplesCountBeforeEmit);
    }
  }

  @Override
  protected TestUser getNextTuple()
  {
    counter++;
    return new TestUser(UUID.randomUUID(), "fName-" + counter, "lName-" + counter, "city-" + counter);
  }

  /**
   * Get size of batch of tuples to emit per window
   * 
   * @return batchSizePerWindow
   */
  public int getBatchSizePerWindow()
  {
    return batchSizePerWindow;
  }

  /**
   * Set size of batch of tuples to emit per window
   * 
   * @param batchSizePerWindow
   */
  public void setBatchSizePerWindow(int batchSizePerWindow)
  {
    this.batchSizePerWindow = batchSizePerWindow;
  }
}
