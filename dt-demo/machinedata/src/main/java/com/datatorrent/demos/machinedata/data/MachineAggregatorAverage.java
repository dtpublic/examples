/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.machinedata.data;

import java.util.List;

import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorAverage;
import org.apache.apex.malhar.lib.dimensions.aggregator.IncrementalAggregator;

import com.datatorrent.api.annotation.Name;
import com.google.common.collect.ImmutableList;

@Name("AVG")
/**
 * @since 3.2.0
 */
public class MachineAggregatorAverage extends AggregatorAverage
{
  private static final long serialVersionUID = 201510130110L;

  /**
   * The singleton instance of this class.
   */
  public static final MachineAggregatorAverage INSTANCE = new MachineAggregatorAverage();

  public static final List<Class<? extends IncrementalAggregator>> MACHINE_CHILD_AGGREGATORS
    = ImmutableList.of((Class<? extends IncrementalAggregator>)MachineAggregatorSum.class,
                       (Class<? extends IncrementalAggregator>)MachineAggregatorCount.class);

  protected MachineAggregatorAverage()
  {
  }

  @Override
  public List<Class<? extends IncrementalAggregator>> getChildAggregators()
  {
    return MACHINE_CHILD_AGGREGATORS;
  }

}
