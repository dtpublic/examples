/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.app;

import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.telecom.model.EnrichedCDR;
import com.datatorrent.demos.dimensions.telecom.operator.EnrichedCDRHbaseInputOperator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

@ApplicationAnnotation(name=TelecomDimensionsDemo.APP_NAME)
public class TelecomDimensionsDemo implements StreamingApplication
{
  public static final String APP_NAME = "TelecomDimensionsDemo";
  public static final String EVENT_SCHEMA = "telecomDimensionsEventSchema.json";
  public static final String PROP_STORE_PATH = "dt.application." + APP_NAME + ".operator.Store.fileStore.basePathPrefix";
  
  public String eventSchemaLocation = EVENT_SCHEMA;
  public EnrichedCDRHbaseInputOperator inputOperator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);

    //input
    if(inputOperator == null)
      inputOperator = new EnrichedCDRHbaseInputOperator();
    dag.addOperator("InputGenerator", inputOperator);

    //dimension
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 4);
    

    //Set operator properties
    //key expression
    {
      Map<String, String> keyToExpression = Maps.newHashMap();
      keyToExpression.put("imsi", "getImsi()");
      keyToExpression.put("Carrier", "getOperatorCode()");
      keyToExpression.put("imei", "getImei()");
      dimensions.setKeyToExpression(keyToExpression);
    }
    
    EnrichedCDR cdr = new EnrichedCDR();
    cdr.getOperatorCode();
    cdr.getDuration();
    
    //aggregate expression
    {
      Map<String, String> aggregateToExpression = Maps.newHashMap();
      aggregateToExpression.put("duration", "getDuration()");
      aggregateToExpression.put("terminatedAbnomally", "getTerminatedAbnomally()" );
      aggregateToExpression.put("terminatedNomally", "getTerminatedNomally()");
      aggregateToExpression.put("called", "getCalled()");
      dimensions.setAggregateToExpression(aggregateToExpression);
    }
    
    //event schema
    dimensions.setConfigurationSchemaJSON(eventSchema);

    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
    dag.getMeta(dimensions).getMeta(dimensions.output).getUnifierMeta().getAttributes().put(OperatorContext.MEMORY_MB, 8092);

    
    //store
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);
    String basePath = conf.get(PROP_STORE_PATH);
    if(basePath == null || basePath.isEmpty())
      basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH), "base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += System.currentTimeMillis();
    hdsFile.setBasePath(basePath);

    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());
    store.setConfigurationSchemaJSON(eventSchema);
    //store.setDimensionalSchemaStubJSON(eventSchema);
   
    PubSubWebSocketAppDataQuery query = createAppDataQuery();
    store.setEmbeddableQueryInfoProvider(query);
  
    //wsOut
    PubSubWebSocketAppDataResult wsOut = createAppDataResult();
    dag.addOperator("QueryResult", wsOut);
    //Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("InputStream", inputOperator.outputPort, dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
  
  protected PubSubWebSocketAppDataQuery createAppDataQuery()
  {
    return new PubSubWebSocketAppDataQuery();
  }
  
  protected PubSubWebSocketAppDataResult createAppDataResult()
  {
    return new PubSubWebSocketAppDataResult();
  }
}

