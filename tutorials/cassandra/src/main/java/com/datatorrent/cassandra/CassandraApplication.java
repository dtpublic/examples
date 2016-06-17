/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.cassandra;

import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.contrib.util.POJOTupleGenerateOperator;
import com.datatorrent.lib.util.FieldInfo;
import com.google.common.collect.Lists;

/**
 * Application to test Cassandra Output and Input Operators from Apache
 * Apex-Malhar library Application has following operators:<br/>
 * 1. TuplesDataGenerator: Generates configured number of tupes for testing (default: 5000).<br/>
 * 2. CassandraDataPopulator: Extends CassandraPojoOutputOperator to write data to cassandra DB.<br/>
 * 3. CassandraDataReader: Extends CassandraPojoInputOperator to read data from cassandra DB.<br/>
 * 4. DataValidator: Validates Generated data against data written and read back from Cassandra DB using Malhar operators.
 * 
 * @author priyanka
 *
 */
@ApplicationAnnotation(name = "CassandraTestApplication")
public class CassandraApplication implements StreamingApplication
{
  private static final int DEFAULT_WRITE_COUNT = 5000;

  public void populateDAG(DAG dag, Configuration conf)
  {
    int tuplesCount = conf.getInt("dt.application.CassandraTestApplication.prop.rowsCount", DEFAULT_WRITE_COUNT);
    boolean validate = conf.getBoolean("dt.application.CassandraTestApplication.prop.validate", true);
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("ID", "id", null));
    fieldInfos.add(new FieldInfo("FNAME", "firstName", null));
    fieldInfos.add(new FieldInfo("LNAME", "lastName", null));
    fieldInfos.add(new FieldInfo("CITY", "city", null));

    PojoGenerator tuplesGenerator = new PojoGenerator();
    tuplesGenerator.setTupleNum(tuplesCount);

    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();

    CassandraDataPopulator cassandraOutput = new CassandraDataPopulator();
    cassandraOutput.setStore(transactionalStore);
    cassandraOutput.setFieldInfos(fieldInfos);
    cassandraOutput.setNumRowsToWrite(tuplesCount);

    CassandraInputOperator cassandraInput = new CassandraInputOperator();
    cassandraInput.setStore(transactionalStore);
    cassandraInput.setFieldInfos(fieldInfos);
    cassandraInput.setTuplesCount(tuplesCount);

    DataValidator dataValidator = new DataValidator();
    dataValidator.setValidate(validate);

    dag.addOperator("TuplesDataGenerator", tuplesGenerator);
    dag.addOperator("CassandraDataPopulator", cassandraOutput);
    dag.addOperator("CassandraDataReader", cassandraInput);
    dag.addOperator("DataValidator", dataValidator);

    dag.addStream("tuplesToDatabase", tuplesGenerator.outputPort, cassandraOutput.tuplesInput);
    dag.addStream("scanTrigger", cassandraOutput.validatationInfo, cassandraInput.scanTrigger);
    dag.addStream("pojoStream", cassandraInput.outputPort, dataValidator.pojoInput).setLocality(Locality.THREAD_LOCAL);
    dag.addStream("validationInfo", cassandraInput.validatationInfo, dataValidator.validationInfoInput).setLocality(
        Locality.THREAD_LOCAL);

  }
}

/**
 * Generates Test pojo tuples.
 *
 */
class PojoGenerator extends POJOTupleGenerateOperator<TestUsers>
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
  protected TestUsers getNextTuple()
  {
    counter++;
    return new TestUsers(UUID.randomUUID(), "fName-" + counter, "lName-" + counter, "city-" + counter);
  }

  /**
   * Get size of batch of tuples to emit per window
   * @return batchSizePerWindow
   */
  public int getBatchSizePerWindow()
  {
    return batchSizePerWindow;
  }

  /**
   * Set size of batch of tuples to emit per window
   * @param batchSizePerWindow
   */
  public void setBatchSizePerWindow(int batchSizePerWindow)
  {
    this.batchSizePerWindow = batchSizePerWindow;
  }
}
