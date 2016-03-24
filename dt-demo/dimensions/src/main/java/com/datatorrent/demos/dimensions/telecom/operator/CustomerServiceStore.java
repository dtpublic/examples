/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.DimensionsDescriptor;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.EventKey;
import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;

public class CustomerServiceStore extends AppDataSingleSchemaDimensionStoreHDHTUpdateWithList
{
  private static final transient Logger logger = LoggerFactory.getLogger(CustomerServiceStore.class);
  
  private static final long serialVersionUID = -7354676382869813092L;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Map<String, Long>>> satisfactionRatingOutputPort = new DefaultOutputPort<>();
  
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Map<String, Long>>> averageWaitTimeOutputPort = new DefaultOutputPort<>();
  
  //service call don't need extra information
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> serviceCallOutputPort = new DefaultOutputPort<>();
  
  //1 minute
  protected transient int timeBucket;
  protected static final String TIME_FIELD_NAME = "time";
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    timeBucket = this.configurationSchema.getCustomTimeBucketRegistry().getTimeBucketId(new CustomTimeBucket(TimeBucket.MINUTE));
  }
  
  @Override
  protected void emitUpdates()
  {
    super.emitUpdates();
    emitUpdatesAverageFor("satisfaction", 0, satisfactionRatingOutputPort);
    emitUpdatesAverageFor("wait", 0, averageWaitTimeOutputPort);
  }
  
  final protected Map<String, Long> fieldValue = Maps.newHashMap();
  @SuppressWarnings("unchecked")
  final protected List<Map<String, Long>> averageTuple = Lists.newArrayList(fieldValue);
  protected void emitUpdatesAverageFor(String fieldName, int dimensionDesciptionId, DefaultOutputPort<List<Map<String, Long>>> output)
  {
    if(!output.isConnected())
      return;

    long sum = 0;
    long count = 0;
    for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
//      logger.debug("\nentry.getKey().getDimensionDescriptorID(): {},  dimensionDesciptionId: {}; \n"
//          + "entry.getKey().getKey().getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET): {}, timeBucket: {}; \n"
//          + "entry.getKey().getKey().getFieldLong(TIME_FIELD_NAME): {}, getMaxTimestamp(): {}", 
//          entry.getKey().getDimensionDescriptorID(), dimensionDesciptionId, 
//          entry.getKey().getKey().getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET), timeBucket, 
//          entry.getKey().getKey().getFieldLong(TIME_FIELD_NAME), getMaxTimestamp()); 
      //check the dimension id;
      if(entry.getKey().getDimensionDescriptorID() != dimensionDesciptionId)
        continue;
      //time bucket is 1 minute
      if(entry.getKey().getKey().getFieldInt(DimensionsDescriptor.DIMENSION_TIME_BUCKET) != timeBucket)
        continue;
      //the time is most recent time
      long time = entry.getKey().getKey().getFieldLong(TIME_FIELD_NAME);
      if(time != getMaxTimestamp())
        continue;
      
      //this is the qualified entry 
      int aggregatorId = entry.getKey().getAggregatorID();
      if(AggregatorIncrementalType.COUNT.ordinal() == aggregatorId)
        count = entry.getValue().getAggregates().getFieldLong(fieldName);
      if(AggregatorIncrementalType.SUM.ordinal() == aggregatorId)
        sum = entry.getValue().getAggregates().getFieldLong(fieldName);
      
      if(sum != 0 && count != 0)
        break;
    }
    if(count != 0)
    {
      fieldValue.clear();
      //fieldValue.put(fieldName, sum/count);
      fieldValue.put("current", sum/count);   //change the field name to current
      output.emit(averageTuple);
    }
    logger.info("field name: {}; sum={}; count={}", fieldName, sum, count); 
  }

  @Override
  protected DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID, int dimensionDescriptorID)
  {
    return serviceCallOutputPort;
  }
  

}
