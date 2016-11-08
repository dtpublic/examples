/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.demos.dimensions.telecom.generate.CustomerInfoRandomGenerator;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo;
import com.datatorrent.demos.dimensions.telecom.model.CustomerEnrichedInfo.SingleRecord;

public class CustomerEnrichedInfoGenerateOperator implements InputOperator
{
  public final transient DefaultOutputPort<CustomerEnrichedInfo.SingleRecord> outputPort = new DefaultOutputPort<CustomerEnrichedInfo.SingleRecord>();

  private int batchSize = 10;
  //the total amount of customers to generate.
  private int customerSize = 30000;
  private transient CustomerInfoRandomGenerator generator = new CustomerInfoRandomGenerator();
  private int generatedSize = 0;

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    if (generatedSize >= customerSize) {
      return;
    }
    for (int i = 0; i < batchSize;) {
      CustomerEnrichedInfo enrichedInfo = new CustomerEnrichedInfo(generator.next());
      SingleRecord[] records = enrichedInfo.getRecords();
      for (SingleRecord record : records) {
        outputPort.emit(record);
        ++i;
        ++generatedSize;
        if (generatedSize >= customerSize || i >= batchSize) {
          return;
        }
      }
    }
  }

  public int getCustomerSize()
  {
    return customerSize;
  }

  public void setCustomerSize(int customerSize)
  {
    this.customerSize = customerSize;
  }

}
