/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata;

import java.util.Map;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;
import org.apache.apex.malhar.lib.dimensions.aggregator.OTFAggregator;

import com.google.common.collect.Maps;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.demos.machinedata.data.MachineAggregatorAverage;
import com.datatorrent.demos.machinedata.data.MachineAggregatorCount;
import com.datatorrent.demos.machinedata.data.MachineAggregatorSum;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;

@ApplicationAnnotation(name = ApplicationPerformant.APP_NAME)
/**
 * @since 3.2.0
 */
public class ApplicationPerformant extends Application
{
  public static final String APP_NAME = "MachineDataDemoPerformant";

  public static final AggregatorRegistry MACHINE_REGISTRY = initRegistry();

  private static AggregatorRegistry initRegistry()
  {
    Map<String, IncrementalAggregator> nameToIncrementalAggregator = Maps.newHashMap();
    nameToIncrementalAggregator.put("SUM", new MachineAggregatorSum());
    nameToIncrementalAggregator.put("COUNT", new MachineAggregatorCount());

    Map<String, Integer> incrementalAggregatorNameToID = Maps.newHashMap();
    incrementalAggregatorNameToID.put("SUM", 1);
    incrementalAggregatorNameToID.put("COUNT", 2);

    Map<String, OTFAggregator> nameToOTFAggregator = Maps.newHashMap();
    nameToOTFAggregator.put("AVG", MachineAggregatorAverage.INSTANCE);

    return new AggregatorRegistry(nameToIncrementalAggregator, nameToOTFAggregator, incrementalAggregatorNameToID);
  }

  @Override
  public DimensionsComputationFlexibleSingleSchemaPOJO getDimensions()
  {
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = new DimensionsComputationFlexibleSingleSchemaPOJO();
    dimensions.setAggregatorRegistry(MACHINE_REGISTRY);
    return dimensions;
  }

  @Override
  public AppDataSingleSchemaDimensionStoreHDHT getStore()
  {
    AppDataSingleSchemaDimensionStoreHDHT store = new AppDataSingleSchemaDimensionStoreHDHT();
    store.setAggregatorRegistry(MACHINE_REGISTRY);
    return store;
  }

}
