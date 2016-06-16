/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.EventKey;
import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;

public abstract class AppDataSingleSchemaDimensionStoreHDHTUpdateWithList extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSingleSchemaDimensionStoreHDHTUpdateWithList.class);

  private static final long serialVersionUID = -5870578159232945511L;
  protected static final String TIME_FIELD_NAME = "time";

  /*
   * a list of pair of (aggregatorID, dimensionDescriptorID)
   * the DescriptorID is combination of ( time-bucket and dimensions ), start with zero. see schema
   */
  protected List<MutablePair<Integer, Integer>> aggregatorsInfo;

  //private int aggregatorID;

  protected transient List<Aggregate> updatingAggregates = Lists.newArrayList();

  @Override
  protected void emitUpdates()
  {
    super.emitUpdates();

    for (int index = 0; index < aggregatorsInfo.size(); ++index) {
      MutablePair<Integer, Integer> info = aggregatorsInfo.get(index);
      if (info == null) {
        continue;
      }

      int aggregatorID = info.left;
      int dimensionDescriptorID = info.right;
      DefaultOutputPort<List<Aggregate>> outputPort = getOutputPort(index++, aggregatorID, dimensionDescriptorID);
      if (outputPort == null || !outputPort.isConnected()) {
        continue;
      }

      updatingAggregates.clear();

      for (Map.Entry<EventKey, Aggregate> entry : cache.entrySet()) {
        if (aggregatorID == entry.getKey().getAggregatorID()
            && entry.getKey().getDimensionDescriptorID() == dimensionDescriptorID
            && getMaxTimestamp() == entry.getKey().getKey().getFieldLong(TIME_FIELD_NAME)) {
          updatingAggregates.add(entry.getValue());
        }
      }

      if (!updatingAggregates.isEmpty()) {
        outputPort.emit(updatingAggregates);
      }
    }
  }

  protected abstract DefaultOutputPort<List<Aggregate>> getOutputPort(int index, int aggregatorID,
      int dimensionDescriptorID);

  public void addAggregatorsInfo(int aggregatorID, int dimensionDescriptorID)
  {
    if (aggregatorsInfo == null) {
      aggregatorsInfo = Lists.newArrayList();
    }
    aggregatorsInfo.add(new MutablePair<Integer, Integer>(aggregatorID, dimensionDescriptorID));
  }

  public List<MutablePair<Integer, Integer>> getAggregatorsInfo()
  {
    return aggregatorsInfo;
  }

  public void setAggregatorsInfo(List<MutablePair<Integer, Integer>> aggregatorsInfo)
  {
    this.aggregatorsInfo = aggregatorsInfo;
  }

  public void setAggregatorInfo(int index, int aggregatorID, int dimensionDescriptorID)
  {
    if (aggregatorsInfo == null) {
      aggregatorsInfo = Lists.newArrayList();
    }
    while (aggregatorsInfo.size() <= index) {
      aggregatorsInfo.add(null); //add the null item.
    }

    aggregatorsInfo.set(index, new MutablePair<Integer, Integer>(aggregatorID, dimensionDescriptorID));
  }

}
