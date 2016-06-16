/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.contrib.hbase.HBaseFieldInfo;
import com.datatorrent.contrib.hbase.HBasePOJOInputOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.demos.dimensions.telecom.conf.CustomerServiceHBaseConf;
import com.datatorrent.demos.dimensions.telecom.conf.DataWarehouseConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService.IssueType;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceHbaseOutputOperator;
import com.datatorrent.lib.testbench.ArrayListTestSink;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.TableInfo;


public class CustomerServiceHbaseOutputOperatorTester
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceHbaseOutputOperatorTester.class);
  
  protected DataWarehouseConfig hbaseConfig = CustomerServiceHBaseConf.instance();
  protected long CAPACITY = 1000;
  
  protected long timeOutTime = 100000;
  protected transient TupleCacheOperator<CustomerService> cacheOperator;
  protected List<CustomerService> generatedDataList;
  protected transient TupleCacheOperator<Object> hbaseInputCacheOperator;
  protected List<Object> readDataList;
  
  protected SpecificCustomerServiceGenerateOperator customerServiceGenerator;
  
  @SuppressWarnings("unchecked")
  @Test
  public void test() throws Exception
  {
    CustomerServiceHBaseConf.instance().setHost("localhost");
    TelecomDemoConf.instance.setCdrDir("target/CDR");
    
    {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();
      Configuration conf = new Configuration(false);
  
      populateOutputDAG(dag, conf);
  
      // Create local cluster
      final LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      
      waitMills(1000);
      
      //main thread wait for signal
      final int checkTimePeriod = 200;
      for (int index = 0; index < timeOutTime / checkTimePeriod + timeOutTime % checkTimePeriod; ++index) {
        if (customerServiceGenerator.isTerminated()) {
          break;
        }
        waitMills(checkTimePeriod);
      }
      logger.info("Send Tuples done. going to terminate the application.");
      //make sure this last tuple and end-window handled.
      waitMills(1000);
      generatedDataList = cacheOperator.getCacheData();
      lc.shutdown();
    }

    //start input dag to read data from HBase
    {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();
      Configuration conf = new Configuration(false);
  
      populateInputDAG(dag, conf);
   // Create local cluster
      final LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      waitMills(1000);
      
      int readSize = 0;
      final int checkTimePeriod = 1000;
      for (int index = 0; index < timeOutTime / checkTimePeriod + timeOutTime % checkTimePeriod; ++index) {
        waitMills(checkTimePeriod);
        readDataList = hbaseInputCacheOperator.getCacheData();
        //the startup probably take a while
        if (readSize == readDataList.size() && readSize != 0) {
          break;
        }
        readSize = readDataList.size();
      }
      lc.shutdown();
    }
    
    verify();
  }

  /**
   * this is the DAG for write tuples into HBase
   * @param dag
   * @param conf
   */
  protected void populateOutputDAG(DAG dag, Configuration conf)
  {
    customerServiceGenerator = new SpecificCustomerServiceGenerateOperator();
    customerServiceGenerator.capacity = CAPACITY;
    
    dag.addOperator("CustomerService-Generator", customerServiceGenerator);

    cacheOperator = new TupleCacheOperator<>("cacheOperatorData");
    dag.addOperator("Cache", cacheOperator);
    
    dag.addStream("GenerateStream", customerServiceGenerator.outputPort, cacheOperator.inputPort).setLocality(Locality.CONTAINER_LOCAL);

    {
      CustomerServiceHbaseOutputOperator hbaseOutput = new CustomerServiceHbaseOutputOperator();
      hbaseOutput.setStartOver(true);  //remove old table and create new
      
      dag.addOperator("CustomerService-Output", hbaseOutput);
  
      dag.addStream("CustomerService", cacheOperator.outputPort, hbaseOutput.input).setLocality(Locality.CONTAINER_LOCAL);
    }
  }
  
  /**
   * this is the DAG to read tuples from HBase
   * @param dag
   * @param conf
   */
  protected void populateInputDAG(DAG dag, Configuration conf)
  {
    HBasePOJOInputOperator pojoInput = new HBasePOJOInputOperator();
    pojoInput.setStore(createHBaseStore());
    pojoInput.setPojoTypeName(LoadedCustomerService.class.getName());
    
    {
      TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();
      
      tableInfo.setRowOrIdExpression("imsi");
 
      final String familyName = "f1";
      List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
      fieldsInfo.add( new HBaseFieldInfo( "totalDuration", "totalDuration", SupportType.INTEGER, familyName) );
      fieldsInfo.add( new HBaseFieldInfo( "wait", "wait", SupportType.INTEGER, familyName) );
      fieldsInfo.add( new HBaseFieldInfo( "zipCode", "zipCode", SupportType.STRING, familyName) );
      fieldsInfo.add( new HBaseFieldInfo( "issueType", "issueType", SupportType.STRING, familyName) );
      fieldsInfo.add( new HBaseFieldInfo( "satisfied", "satisfied", SupportType.BOOLEAN, familyName) );
      
      tableInfo.setFieldsInfo(fieldsInfo);
      
      pojoInput.setTableInfo(tableInfo);
    }

    dag.addOperator("HbaseInput", pojoInput);
    
    hbaseInputCacheOperator = new TupleCacheOperator<>("hbaseInputCacheOperatorData");
    
    dag.addOperator("InputCache", hbaseInputCacheOperator);
    hbaseInputCacheOperator.outputPort.setSink(new ArrayListTestSink());
    
    dag.addStream("InputStream", pojoInput.outputPort, hbaseInputCacheOperator.inputPort).setLocality(Locality.CONTAINER_LOCAL);
  }
  
  protected HBaseStore createHBaseStore()
  {
    //store
    HBaseStore store = new HBaseStore();
    store.setTableName(hbaseConfig.getTableName());
    store.setZookeeperQuorum(hbaseConfig.getHost());
    store.setZookeeperClientPort(hbaseConfig.getPort());
    return store;
  }
  
  protected void verify() throws Exception
  {
    Assert.assertFalse("No tuple found in cache tuple.", generatedDataList == null || generatedDataList.isEmpty() );
    logger.info("data size from cache: {}", generatedDataList.size() );
    
    //the data saved to the HBase is key value.
    Map<String, CustomerService> imsiToCsMap = Maps.newHashMap();
    for (CustomerService cs : generatedDataList) {
      imsiToCsMap.put(cs.imsi, cs);
    }
    
    logger.info("expected dataSet size: {}", imsiToCsMap.size());
    
    int fetchedCount = 0;
    for (Object readData : readDataList) {
      LoadedCustomerService lcs = (LoadedCustomerService)readData;
      Assert.assertTrue("Don't have data: " + lcs, lcs.equals(imsiToCsMap.get(lcs.imsi)));
      fetchedCount++;
    }
    
    Assert.assertTrue("The number of records read from HBase (" + fetchedCount + ") is not same as expected (" + imsiToCsMap.size() + ").", fetchedCount == imsiToCsMap.size());
  }
  
  public static void waitMills(int millSeconds)
  {
    if (millSeconds <= 0) {
      return;
    }
    try {
      Thread.sleep(millSeconds);
    } catch (Exception e) {
      //ignore
    }
  }
  

  
  public static class SpecificCustomerServiceGenerateOperator extends CustomerServiceGenerateOperator
  {
    protected String dataId = "SpecificCustomerServiceGenerateOperator";
    protected long capacity = 100;
    protected AtomicBoolean terminateFlag;
    protected DataWrapper<AtomicBoolean> dataWrapper = DataWrapper.getOrCreateInstanceOfId(dataId);
    protected long size = 0;
    protected final int batchSize = 1;
    
    public SpecificCustomerServiceGenerateOperator()
    {
      this.setBatchSleepTime(0);
      this.setBatchSize(batchSize);
    }
    
    @Override
    public void setup(OperatorContext context)
    {
      terminateFlag = dataWrapper.getOrSetData(new AtomicBoolean(false));
      dataWrapper.syncData();
    }

    @Override
    public void emitTuples()
    {
      if (size >= capacity) {
        terminateFlag.set(true);
        waitMills(2);
        return;
      }
      super.emitTuples();
      size += batchSize;
    }
    
    public boolean isTerminated()
    {
      return dataWrapper.getData() != null && dataWrapper.getData().get();
    }
  }
  
  /**
   * Keep the information loaded from HBase.
   * NOTES: not all field of CustomerService be kept in the HBase.
   * 
   *
   */
  public static class LoadedCustomerService
  {
    protected String imsi;
    protected int totalDuration;
    protected int wait;
    protected String zipCode;
    protected String issueType;
    protected boolean satisfied;
    
    public LoadedCustomerService(){}
    
    public LoadedCustomerService(String imsi, int totalDuration, int wait, String zipCode, String issueType,
        boolean satisfied)
    {
      this.imsi = imsi;
      this.totalDuration = totalDuration;
      this.wait = wait;
      this.zipCode = zipCode;
      this.issueType = issueType;
      this.satisfied = satisfied;
    }

    public boolean equals(Object obj)
    {
      if (obj == null || !(obj instanceof CustomerService)) {
        return false;
      }

      CustomerService cs = (CustomerService)obj;
      return imsi.equals(cs.imsi) && totalDuration == cs.totalDuration && wait == cs.wait && zipCode.equals(zipCode) && IssueType.valueOf(issueType) == cs.issueType && satisfied == cs.satisfied;
    }

    public String getImsi()
    {
      return imsi;
    }

    public void setImsi(String imsi)
    {
      this.imsi = imsi;
    }

    public int getTotalDuration()
    {
      return totalDuration;
    }

    public void setTotalDuration(int totalDuration)
    {
      this.totalDuration = totalDuration;
    }

    public int getWait()
    {
      return wait;
    }

    public void setWait(int wait)
    {
      this.wait = wait;
    }

    public String getZipCode()
    {
      return zipCode;
    }

    public void setZipCode(String zipCode)
    {
      this.zipCode = zipCode;
    }

    public String getIssueType()
    {
      return issueType;
    }

    public void setIssueType(String issueType)
    {
      this.issueType = issueType;
    }

    public boolean isSatisfied()
    {
      return satisfied;
    }

    public void setSatisfied(boolean satisfied)
    {
      this.satisfied = satisfied;
    }
  }
}
