/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;

public class CDRStore extends AppDataSingleSchemaDimensionStoreHDHTUpdateWithList
{
  private static final long serialVersionUID = 2348875268413944860L;

  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<List<Aggregate>> updateWithList = new DefaultOutputPort<>();

  @Override
  protected DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID, int dimensionDescriptorID)
  {
    return updateWithList;
  }
  
}
