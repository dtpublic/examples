/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

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
