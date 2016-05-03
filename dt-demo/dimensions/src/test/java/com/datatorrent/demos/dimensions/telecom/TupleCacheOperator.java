/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.List;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Lists;

public class TupleCacheOperator<T> extends BaseOperator
{
  public static final String DEFAULT_DATA_ID = "TupleCacheOperator";
  
  private String dataId;
  /**
   * it could have multiple instance of TupleCacheOperator.
   * The user of DataWrapper should distinguish if it's just a clone or it's really a new instance.
   * 
   */
  protected DataWrapper<List<T>> dataWrapper;
  protected List<T> dataList;
  
  public TupleCacheOperator()
  {
    this(DEFAULT_DATA_ID);
  }
  
  public TupleCacheOperator(String dataId)
  {
    this.dataId = dataId;
    dataWrapper = DataWrapper.getOrCreateInstanceOfId(dataId);
    dataList = dataWrapper.getOrSetData(Lists.<T>newArrayList());    
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public void setup(OperatorContext context)
  {
    dataWrapper.syncData();
  }

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T t)
    {
      processTuple(t);
    }
  };

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  
  protected void processTuple(T tuple)
  {
    dataList.add(tuple);
    if(outputPort.isConnected())
      outputPort.emit(tuple);
  }

  @SuppressWarnings("unchecked")
  public List<T> getCacheData()
  {
    return dataWrapper.getData();
  }
  
}
