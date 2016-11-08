/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.dimensions.CombinationDimensionalExpander;
import com.datatorrent.contrib.dimensions.CombinationValidator;
import com.datatorrent.contrib.dimensions.DimensionsQueueManager;

/**
 * 
 * @author bright
 *
 */
public class GeoDimensionStore extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final long serialVersionUID = 3839563720592204620L;

  protected RegionZipCombinationFilter filter = new RegionZipCombinationFilter();
  protected RegionZipCombinationValidator validator = new RegionZipCombinationValidator();

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected DimensionsQueueManager getDimensionsQueueManager()
  {
    return new DimensionsQueueManager(this, schemaRegistry, new CombinationDimensionalExpander((Map)seenEnumValues).withCombinationFilter(filter).withCombinationValidator((CombinationValidator)validator));
  }

}
