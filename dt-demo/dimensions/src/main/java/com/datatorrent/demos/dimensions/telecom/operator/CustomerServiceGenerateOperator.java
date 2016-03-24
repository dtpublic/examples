/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.telecom.generate.CustomerServiceDefaultGenerator;
import com.datatorrent.demos.dimensions.telecom.model.CustomerService;

public class CustomerServiceGenerateOperator implements InputOperator {
  public final transient DefaultOutputPort<CustomerService> outputPort = new DefaultOutputPort<CustomerService>();

  private int batchSize = 10;
  private int batchSleepTime = 2;
  private CustomerServiceDefaultGenerator generator = new CustomerServiceDefaultGenerator();
  
  @Override
  public void beginWindow(long windowId) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void endWindow() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void setup(OperatorContext context) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void teardown() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void emitTuples() {
    for(int i=0; i<batchSize; ++i)
    {
      outputPort.emit(generator.next());
    }
    if(batchSleepTime > 0)
    {
      try
      {
        Thread.sleep(batchSleepTime);
      }
      catch(Exception e){}
    }
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchSleepTime()
  {
    return batchSleepTime;
  }

  public void setBatchSleepTime(int batchSleepTime)
  {
    this.batchSleepTime = batchSleepTime;
  }
  

}
