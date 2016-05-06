/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.app;

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorIncrementalType;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.DimensionStoreHDHTNonEmptyQueryResultUnifier;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.contrib.hive.HiveStore;
import com.datatorrent.demos.dimensions.telecom.conf.ConfigUtil;
import com.datatorrent.demos.dimensions.telecom.conf.EnrichedCustomerServiceHiveConfig;
import com.datatorrent.demos.dimensions.telecom.conf.TelecomDemoConf;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveExecuteOperator;
import com.datatorrent.demos.dimensions.telecom.hive.TelecomHiveOutputOperator;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCustomerService;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSimpleConfigurableSnapshotServer;
import com.datatorrent.demos.dimensions.telecom.operator.AppDataSnapshotServerAggregate;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceEnrichOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceGenerateOperator;
import com.datatorrent.demos.dimensions.telecom.operator.CustomerServiceStore;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCustomerServiceCassandraOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCustomerServiceHbaseOutputOperator;
import com.datatorrent.demos.dimensions.telecom.operator.GeoDimensionStore;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 *
 * # of service calls by Zipcode
 * Top 10 Zipcodes by Service Calls -> Drill Down to get Customer records
 * # Total wait time v/s Average Wait time for Top 10 Zipcodes
 * I also want running wait times for all zipcodes
 *
 * @author bright
 *
 */
@ApplicationAnnotation(name = CustomerServiceDemoV2.APP_NAME)
public class CustomerServiceDemoV2 implements StreamingApplication {
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceDemoV2.class);

  public static final String APP_NAME = "CustomerServiceDemoV2";
  public static final String CS_DIMENSION_SCHEMA = "customerServiceDemoV2EventSchema.json";
  public static final String CS_GEO_SCHEMA = "csGeoSchema.json";
  public static final String SERVICE_CALL_SCHEMA = "serviceCallSnapshotSchema.json";
  public static final String SATISFACTION_RATING_SCHEMA = "satisfactionRatingSnapshotSchema.json";
  public static final String AVERAGE_WAITTIME_SCHEMA = "averageWaittimeSnapshotSchema.json";
  public static final String BULLETIN_TAG = "bulletin";

  public final String appName;
  protected String PROP_STORE_PATH;
  protected String PROP_GEO_STORE_PATH;
  protected String PROP_CASSANDRA_HOST;
  protected String PROP_HBASE_HOST;
  protected String PROP_HIVE_HOST;
  protected String PROP_OUTPUT_MASK;
  protected String PROP_HIVE_TEMP_PATH;
  protected String PROP_HIVE_TEMP_FILE;
  protected String PROP_CSSTORE_PARTITIONCOUNT;
  protected String PROP_CSGEOSTORE_PARTITIONCOUNT;

  public static final int outputMask_HBase = 0x01;
  public static final int outputMask_Hive = 0x02;
  public static final int outputMask_Cassandra = 0x04;

  protected int outputMask = outputMask_Cassandra;

  protected String eventSchemaLocation = CS_DIMENSION_SCHEMA;
  protected String csGeoSchemaLocation = CS_GEO_SCHEMA;
  protected String serviceCallSchemaLocation = SERVICE_CALL_SCHEMA;
  protected String satisfactionRatingSchemaLocation = SATISFACTION_RATING_SCHEMA;
  protected String averageWaittimeSchemaLocation = AVERAGE_WAITTIME_SCHEMA;

  protected boolean enableDimension = true;
  protected boolean enableGeo = true;
  protected String hiveTmpPath = "/user/cstmp";
  protected String hiveTmpFile = "cs";
  protected String enrichedCSTableSchema
  = "CREATE TABLE IF NOT EXISTS %s ( imsi string, isdn string, imei string, totalDuration string, wait string, zipCode string, " +
    " issueType string, satisfied string, operatorCode string, deviceBrand string,  deviceModel string ) " +
    " PARTITIONED BY( createdtime long ) " +
    " ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\"";

  protected int csStorePartitionCount = 2;
  protected int csGeoStorePartitionCount = 2;

  public CustomerServiceDemoV2()
  {
    this(APP_NAME);
  }

  public CustomerServiceDemoV2(String appName)
  {
    this.appName = appName;
    PROP_CASSANDRA_HOST = "dt.application." + appName + ".cassandra.host";
    PROP_HBASE_HOST = "dt.application." + appName + ".hbase.host";
    PROP_HIVE_HOST = "dt.application." + appName + ".hive.host";
    PROP_STORE_PATH = "dt.application." + appName + ".operator.StoreServiceKPIs.fileStore.basePathPrefix";
    PROP_GEO_STORE_PATH = "dt.application." + appName + ".operator.StoreTaggedServiceGeoLocations.fileStore.basePathPrefix";
    PROP_OUTPUT_MASK = "dt.application." + appName + ".csoutputmask";
    PROP_HIVE_TEMP_PATH = "dt.application." + appName + ".cshivetmppath";
    PROP_HIVE_TEMP_FILE = "dt.application." + appName + ".cshivetmpfile";
    PROP_CSSTORE_PARTITIONCOUNT = "dt.application." + appName + ".csStorePartitionCount";
    PROP_CSGEOSTORE_PARTITIONCOUNT = "dt.application." + appName + ".csGeoStorePartitionCount";
  }

  protected void populateConfig(Configuration conf)
  {
    {
      final String sOutputMask = conf.get(PROP_OUTPUT_MASK);
      if(sOutputMask != null)
      {
        try
        {
          outputMask = Integer.valueOf(sOutputMask);
          logger.info("outputMask: {}", outputMask);
        }
        catch(Exception e)
        {
          logger.error("Invalid outputmask: {}", sOutputMask);
        }
      }

    }
    {
      final String cassandraHost = conf.get(PROP_CASSANDRA_HOST);
      if(cassandraHost != null)
      {
        TelecomDemoConf.instance.setCassandraHost(cassandraHost);
      }
      logger.info("CassandraHost: {}", TelecomDemoConf.instance.getCassandraHost());
    }

    {
      final String hbaseHost = conf.get(PROP_HBASE_HOST);
      if(hbaseHost != null)
      {
        TelecomDemoConf.instance.setHbaseHost(hbaseHost);
      }
      logger.info("HbaseHost: {}", TelecomDemoConf.instance.getHbaseHost());
    }

    {
      final String hiveHost = conf.get(PROP_HIVE_HOST);
      if(hiveHost != null)
      {
        TelecomDemoConf.instance.setHiveHost(hiveHost);
      }
      logger.info("HiveHost: {}", TelecomDemoConf.instance.getHiveHost());
    }
    {
      final String hiveTmpPath = conf.get(PROP_HIVE_TEMP_PATH);
      if(hiveTmpPath != null )
        this.hiveTmpPath = hiveTmpPath;
      logger.info("hiveTmpPath: {}", hiveTmpPath);
    }
    {
      final String hiveTmpFile = conf.get(PROP_HIVE_TEMP_FILE);
      if(hiveTmpFile != null )
        this.hiveTmpFile = hiveTmpFile;
      logger.info("hiveTmpFile: {}", hiveTmpFile);
    }

    csStorePartitionCount = conf.getInt(PROP_CSSTORE_PARTITIONCOUNT, csStorePartitionCount);
    csGeoStorePartitionCount = conf.getInt(PROP_CSGEOSTORE_PARTITIONCOUNT, csGeoStorePartitionCount);
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf) {
    populateConfig(conf);
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    // Customer service generator
    CustomerServiceGenerateOperator customerServiceGenerator = new CustomerServiceGenerateOperator();
    dag.addOperator("IngestCustomerServiceData", customerServiceGenerator);

    CustomerServiceEnrichOperator enrichOperator = new CustomerServiceEnrichOperator();
    dag.addOperator("EnrichServiceRecords", enrichOperator);

    dag.addStream("CustomerService", customerServiceGenerator.outputPort, enrichOperator.inputPort);

    List<DefaultInputPort<? super EnrichedCustomerService>> customerServiceStreamSinks = Lists.newArrayList();

    // Customer service persist
    if((outputMask & outputMask_HBase) != 0)
    {
      // HBase
      EnrichedCustomerServiceHbaseOutputOperator customerServicePersist = new EnrichedCustomerServiceHbaseOutputOperator();
      dag.addOperator("CSHBasePersist", customerServicePersist);
      customerServiceStreamSinks.add(customerServicePersist.input);
    }
    if((outputMask & outputMask_Cassandra) != 0)
    {
      // Cassandra
      EnrichedCustomerServiceCassandraOutputOperator customerServicePersist = new EnrichedCustomerServiceCassandraOutputOperator();
      //dag.addOperator("CustomerService-Cassandra-Persist", customerServicePersist);
      dag.addOperator("CSCassandraPersist", customerServicePersist);
      customerServiceStreamSinks.add(customerServicePersist.input);
    }
    if((outputMask & outputMask_Hive) != 0)
    {
      TelecomHiveOutputOperator hiveOutput = new TelecomHiveOutputOperator();
      if(hiveTmpPath != null)
        hiveOutput.setFilePath(hiveTmpPath);
      if(hiveTmpFile != null)
        hiveOutput.setOutputFileName(hiveTmpFile);
      hiveOutput.setFilePermission((short)511);

      dag.addOperator("CSHiveOutput", hiveOutput);
      customerServiceStreamSinks.add(hiveOutput.input);

      TelecomHiveExecuteOperator hiveExecute = new TelecomHiveExecuteOperator();

      {
        HiveStore hiveStore = new HiveStore();
        if(hiveTmpPath != null)
          hiveStore.setFilepath(hiveTmpPath);
        hiveExecute.setHivestore(hiveStore);
      }
      hiveExecute.setHiveConfig(EnrichedCustomerServiceHiveConfig.instance());
      String createTableSql = String.format( enrichedCSTableSchema,  EnrichedCustomerServiceHiveConfig.instance().getDatabase() + "." + EnrichedCustomerServiceHiveConfig.instance().getTableName() );
      hiveExecute.setCreateTableSql(createTableSql);
      dag.addOperator("CSHiveExecute", hiveExecute);
      dag.addStream("CSHiveLoadData", hiveOutput.hiveCmdOutput, hiveExecute.input);
    }

    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = null;
    if (enableDimension) {
      // dimension
      dimensions = dag.addOperator("ComputeServiceKPIs",
          DimensionsComputationFlexibleSingleSchemaPOJO.class);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
      dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
      customerServiceStreamSinks.add(dimensions.input);

      // Set operator properties
      // key expression
      {
        Map<String, String> keyToExpression = Maps.newHashMap();
        keyToExpression.put("zipCode", "getZipCode()");
        keyToExpression.put("issueType", "getIssueType()");
        keyToExpression.put("time", "getTime()");
        dimensions.setKeyToExpression(keyToExpression);
      }

      // aggregate expression
      {
        Map<String, String> aggregateToExpression = Maps.newHashMap();
        aggregateToExpression.put("serviceCall", "getServiceCallCount()");
        aggregateToExpression.put("wait", "getWait()");
        aggregateToExpression.put("satisfaction", "getSatisfaction()");
        dimensions.setAggregateToExpression(aggregateToExpression);
      }

      // event schema
      dimensions.setConfigurationSchemaJSON(eventSchema);

      dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
      dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
          8092);

      // store
      CustomerServiceStore store = dag.addOperator("StoreServiceKPIs", CustomerServiceStore.class);
      store.setUpdateEnumValues(true);
      String basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
            "base path should be specified in the properties.xml");
      TFileImpl hdsFile = new TFileImpl.DTFileImpl();
      basePath += System.currentTimeMillis();
      hdsFile.setBasePath(basePath);

      store.setFileStore(hdsFile);
      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());
      store.setConfigurationSchemaJSON(eventSchema);
      //for service call
      store.addAggregatorsInfo(AggregatorIncrementalType.COUNT.ordinal(), 5);

      PubSubWebSocketAppDataQuery query = createAppDataQuery();
      URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
      logger.info("QueryUri: {}", queryUri);
      query.setUri(queryUri);
      store.setEmbeddableQueryInfoProvider(query);
      if(csStorePartitionCount > 1)
      {
        store.setPartitionCount(csStorePartitionCount);
        store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
      }
      // wsOut
      PubSubWebSocketAppDataResult wsOut = createAppDataResult();
      wsOut.setUri(queryUri);
      dag.addOperator("QueryResult", wsOut);
      // Set remaining dag options

      dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
          new BasicCounters.LongAggregator<MutableLong>());

      dag.addStream("CSDimensionalStream", dimensions.output, store.input);
      dag.addStream("CSQueryResult", store.queryResult, wsOut.input);

      //snapshot servers
      //ServiceCall by type
      {
        AppDataSnapshotServerAggregate snapshotServer = new AppDataSnapshotServerAggregate();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(serviceCallSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.setEventSchema(eventSchema);
        {
          Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap = Maps.newHashMap();
          keyValueMap.put(new MutablePair<String, Type>("issueType", Type.STRING), new MutablePair<String, Type>("serviceCall", Type.LONG));
          snapshotServer.setKeyValueMap(keyValueMap);
        }
        dag.addOperator("ComputeNumOfServiceCalls", snapshotServer);
        dag.addStream("ServiceCallSnapshot", store.serviceCallOutputPort, snapshotServer.input);

        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);
        //dag.addStream("SnapshotQuery", snapShotQuery.outputPort, snapshotServer.query);


        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("ServiceCallQueryResult", snapShotQueryResult);
        dag.addStream("ServiceCallResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }

      //satisfaction rating
      {
        AppDataSimpleConfigurableSnapshotServer snapshotServer = new AppDataSimpleConfigurableSnapshotServer();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(this.satisfactionRatingSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.setTags(Sets.newHashSet(BULLETIN_TAG));
        snapshotServer.addStaticFieldInfo("min", 0L);
        snapshotServer.addStaticFieldInfo("max", 100L);
        snapshotServer.addStaticFieldInfo("threshold", 80L);

        {
          Map<String, Type> fieldInfo = Maps.newHashMap();
          fieldInfo.put("current", Type.LONG);
          fieldInfo.put("min", Type.LONG);
          fieldInfo.put("max", Type.LONG);
          fieldInfo.put("threshold", Type.LONG);
          snapshotServer.setFieldInfoMap(fieldInfo);
        }
        
        dag.addOperator("ComputeSatisfactionRatings", snapshotServer);
        dag.addStream("Satisfaction", store.satisfactionRatingOutputPort, snapshotServer.input);

        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);

        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("SatisfactionQueryResult", snapShotQueryResult);
        dag.addStream("SatisfactionQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }

      //Wait time
      {
        AppDataSimpleConfigurableSnapshotServer snapshotServer = new AppDataSimpleConfigurableSnapshotServer();
        String snapshotServerJSON = SchemaUtils.jarResourceFileToString(this.averageWaittimeSchemaLocation);
        snapshotServer.setSnapshotSchemaJSON(snapshotServerJSON);
        snapshotServer.setTags(Sets.newHashSet(BULLETIN_TAG));
        snapshotServer.addStaticFieldInfo("min", 0L);
        snapshotServer.addStaticFieldInfo("max", 200L);
        snapshotServer.addStaticFieldInfo("threshold", 30L);
        {
          Map<String, Type> fieldInfo = Maps.newHashMap();
          fieldInfo.put("current", Type.LONG);
          fieldInfo.put("min", Type.LONG);
          fieldInfo.put("max", Type.LONG);
          fieldInfo.put("threshold", Type.LONG);
          snapshotServer.setFieldInfoMap(fieldInfo);
        }
        dag.addOperator("ComputeWaitTimes", snapshotServer);
        dag.addStream("Waittime", store.averageWaitTimeOutputPort, snapshotServer.input);

        PubSubWebSocketAppDataQuery snapShotQuery = new PubSubWebSocketAppDataQuery();
        snapShotQuery.setUri(queryUri);
        //use the EmbeddableQueryInfoProvider instead to get rid of the problem of query schema when latency is very long
        snapshotServer.setEmbeddableQueryInfoProvider(snapShotQuery);

        PubSubWebSocketAppDataResult snapShotQueryResult = new PubSubWebSocketAppDataResult();
        snapShotQueryResult.setUri(queryUri);
        dag.addOperator("WaittimeQueryResult", snapShotQueryResult);
        dag.addStream("WaittimeQueryResult", snapshotServer.queryResult, snapShotQueryResult.input);
      }
    }

    if(enableGeo)
      populateCsGeoDAG(dag, conf, customerServiceStreamSinks);

    dag.addStream("CSEnriched", enrichOperator.outputPort, customerServiceStreamSinks.toArray(new DefaultInputPort[0]));
  }

  protected void populateCsGeoDAG(DAG dag, Configuration conf, List<DefaultInputPort<? super EnrichedCustomerService>> customerServiceStreamSinks)
  {
    // dimension
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("TagServiceGeoLocations",
        DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);

    customerServiceStreamSinks.add(dimensions.input);

    // Set operator properties
    // key expression: Point( Lat, Lon )
    {
      Map<String, String> keyToExpression = Maps.newHashMap();
      keyToExpression.put("zipcode", "getZipCode()");
      keyToExpression.put("region", "getRegionZip2()");
      keyToExpression.put("time", "getTime()");
      dimensions.setKeyToExpression(keyToExpression);
    }

    // aggregate expression: disconnect and downloads
    {
      Map<String, String> aggregateToExpression = Maps.newHashMap();
      aggregateToExpression.put("wait", "getWait()");
      aggregateToExpression.put("lat", "getLat()");
      aggregateToExpression.put("lon", "getLon()");
      dimensions.setAggregateToExpression(aggregateToExpression);
    }

    // event schema
    String geoSchema = SchemaUtils.jarResourceFileToString(csGeoSchemaLocation);
    dimensions.setConfigurationSchemaJSON(geoSchema);

    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB,
        8092);

    // store
    //AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("StoreTaggedServiceGeoLocations", AppDataSingleSchemaDimensionStoreHDHT.class);
    GeoDimensionStore store = dag.addOperator("StoreTaggedServiceGeoLocations", GeoDimensionStore.class);
    store.setUpdateEnumValues(true);
    String basePath = Preconditions.checkNotNull(conf.get(PROP_GEO_STORE_PATH),
          "GEO base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += System.currentTimeMillis();
    hdsFile.setBasePath(basePath);

    store.setFileStore(hdsFile);
    store.setConfigurationSchemaJSON(geoSchema);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());


    PubSubWebSocketAppDataQuery query = createAppDataQuery();
    URI queryUri = ConfigUtil.getAppDataQueryPubSubURI(dag, conf);
    query.setUri(queryUri);
    store.setEmbeddableQueryInfoProvider(query);
    if(csGeoStorePartitionCount > 1)
    {
      store.setPartitionCount(csGeoStorePartitionCount);
      store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
    }

    // wsOut
    PubSubWebSocketAppDataResult wsOut = createAppDataResult();
    wsOut.setUri(queryUri);
    dag.addOperator("CSGeoQueryResult", wsOut);
    // Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("CSGeoStream", dimensions.output, store.input);
    dag.addStream("CSGeoQueryResult", store.queryResult, wsOut.input);
  }

  public boolean isEnableDimension() {
    return enableDimension;
  }

  public void setEnableDimension(boolean enableDimension) {
    this.enableDimension = enableDimension;
  }

  protected PubSubWebSocketAppDataQuery createAppDataQuery() {
    return new PubSubWebSocketAppDataQuery();
  }

  protected PubSubWebSocketAppDataResult createAppDataResult() {
    return new PubSubWebSocketAppDataResult();
  }

  public int getOutputMask()
  {
    return outputMask;
  }
  public void setOutputMask(int outputMask)
  {
    this.outputMask = outputMask;
  }

}
