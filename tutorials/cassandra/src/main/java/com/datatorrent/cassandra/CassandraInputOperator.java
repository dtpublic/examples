/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.IdleTimeHandler;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.contrib.cassandra.CassandraPOJOInputOperator;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;

/**
 * 
 * Extends {@link CassandraPOJOInputOperator} Reads data from Cassandra DB.<br/>
 * Operator Ports: <br/>
 * 1. scanTrigger: Receives {@link ScanInformation} from upstream DataPopulator.
 * The operator waits to read data till the trigger is received. Upstream data
 * populator sends the trigger when it is done writing data to database.<br/>
 * 2. validatationInfo: forwards {@link ScanInformation} received on scanTrigger
 * port to downstream operator.
 */
public class CassandraInputOperator extends CassandraPOJOInputOperator implements IdleTimeHandler
{
  private static Logger LOG = LoggerFactory.getLogger(CassandraInputOperator.class);
  private int tuplesCount;
  private static int processedTuples;
  private ScanInformation scanInfo = null;
  private long scanStartTime;
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<ScanInformation> scanTrigger = new DefaultInputPort<ScanInformation>()
  {

    @Override
    public void process(ScanInformation scanInfo)
    {
      CassandraInputOperator.this.scanInfo = scanInfo;
      try {
        Thread.sleep(2000); //Wait for cassandra to write received rows.
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  };

  public transient DefaultOutputPort<ScanInformation> validatationInfo = new DefaultOutputPort<ScanInformation>();

  @Override
  public void emitTuples()
  {
    super.emitTuples();
    try {
      if (processedTuples >= tuplesCount) {
        long scanEndTime = System.currentTimeMillis();
        LOG.info("Time taken to scan " + tuplesCount + " records: " + (scanEndTime - scanStartTime) + " ms");
        Thread.sleep(1000); //wait till validator gets all tuples
        validatationInfo.emit(scanInfo);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

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
  public void handleIdleTime()
  {
    //process records after getting scanInfo till number of records processed == tuplesCount
    //scanInfo is emitted by upstream operator when all data tuples are emitted, scanInfo acts as trigger to start validation
    if (scanInfo != null && processedTuples < tuplesCount) {
      if (processedTuples == 0) {
        scanStartTime = System.currentTimeMillis();
      }
      emitTuples();
    }
  }

  @Override
  public Object getTuple(Row row)
  {
    processedTuples++;
    return super.getTuple(row);

  }

  @Override
  public void teardown()
  {
    Session session = store.getSession();
    if (session != null) {
      session.execute("Truncate TABLE " + store.getKeyspace() + "." + CassandraTransactionalStore.DEFAULT_META_TABLE);
      session.execute("Truncate TABLE " + store.getKeyspace() + "." + getTablename());
      session.close();
    }
    if (store.getCluster() != null) {
      store.getCluster().close();
    }

    super.teardown();
  }

  /**
   * Get tuples count
   * 
   * @return tuplesCount
   */
  public int getTuplesCount()
  {
    return tuplesCount;
  }

  /**
   * Set tuples count
   * 
   * @param tuplesCount
   */
  public void setTuplesCount(int tuplesCount)
  {
    this.tuplesCount = tuplesCount;
  }
}
