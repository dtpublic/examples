/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.tuple.MutablePair;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;

public class AppDataSnapshotServerAggregate extends AbstractAppDataSnapshotServer<Aggregate>
{
  private static final transient Logger logger = LoggerFactory.getLogger(AppDataSnapshotServerAggregate.class);
      
  private String eventSchema;
  
  /**
   * A map from field Key to Value.
   * 
   */
  private Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap;
  
  protected transient GPOMutable staticFields;
  protected transient FieldsDescriptor fieldsDescriptor;
  protected transient DimensionalConfigurationSchema dimensitionSchema;
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
    dimensitionSchema = new DimensionalConfigurationSchema(eventSchema, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }
  
  protected FieldsDescriptor getFieldsDescriptor()
  {
    if (fieldsDescriptor == null) {
      Map<String, Type> fieldToType = Maps.newHashMap();
      for (Map.Entry<MutablePair<String, Type>, MutablePair<String, Type>> entry : keyValueMap.entrySet()) {
        fieldToType.put(entry.getKey().getKey(), entry.getKey().getValue());
        fieldToType.put(entry.getValue().getKey(), entry.getValue().getValue());
      }
      fieldsDescriptor = new FieldsDescriptor(fieldToType);
    }
    return fieldsDescriptor;
  }

  @Override
  public GPOMutable convert(Aggregate aggregate)
  {
    final FieldsDescriptor aggregatesFd = dimensitionSchema
        .getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(aggregate.getDimensionDescriptorID())
        .get(aggregate.getAggregatorID());
    aggregate.getAggregates().setFieldDescriptor(aggregatesFd);

    final FieldsDescriptor keysFd = dimensitionSchema.getDimensionsDescriptorIDToKeyDescriptor()
        .get(aggregate.getDimensionDescriptorID());
    GPOMutable keys = aggregate.getKeys();
    keys.setFieldDescriptor(keysFd);

    /****
     * { Type fieldType = keysFd.getType("deviceModel"); logger.info(
     * "The type of field '{}' is '{}'", "deviceModel", fieldType);
     * 
     * String deviceModel = keys.getFieldString("deviceModel"); Map<String,
     * Type> fieldToType = Maps.newHashMap(); fieldToType.put("deviceModel",
     * Type.STRING); fieldToType.put("downloadBytes", Type.LONG);
     * 
     * FieldsDescriptor fd = new FieldsDescriptor(fieldToType); GPOMutable gpo =
     * new GPOMutable(fd);
     * 
     * gpo.setField("deviceModel", deviceModel); gpo.setField("downloadBytes",
     * .getFieldLong("downloadBytes"));
     * 
     * return gpo; }
     */

    GPOMutable gpo;
    if(staticFields != null)
    {
      gpo = new GPOMutable(staticFields);
    }
    else
      gpo = new GPOMutable(getFieldsDescriptor());

    for (Map.Entry<MutablePair<String, Type>, MutablePair<String, Type>> entry : keyValueMap.entrySet()) {
      for (int i = 0; i < 2; ++i) {
        String fieldName;
        Type type;
        GPOMutable fieldValueSource;
        if (i == 0) {
          fieldName = entry.getKey().getKey();
          type = entry.getKey().getValue();
          fieldValueSource = keys;
        } else {
          fieldName = entry.getValue().getKey();
          type = entry.getValue().getValue();
          fieldValueSource = aggregate.getAggregates();
        }
        
        switch (type) {
          case BOOLEAN:
            gpo.setField(fieldName, fieldValueSource.getFieldBool(fieldName));
            break;
          case STRING:
            gpo.setField(fieldName, fieldValueSource.getFieldString(fieldName));
            break;
          case CHAR:
            gpo.setField(fieldName, fieldValueSource.getFieldChar(fieldName));
            break;
          case DOUBLE:
            gpo.setField(fieldName, fieldValueSource.getFieldDouble(fieldName));
            break;
          case FLOAT:
            gpo.setField(fieldName, fieldValueSource.getFieldFloat(fieldName));
            break;
          case LONG:
            gpo.setField(fieldName, fieldValueSource.getFieldLong(fieldName));
            break;
          case INTEGER:
            gpo.setField(fieldName, fieldValueSource.getFieldInt(fieldName));
            break;
          case SHORT:
            gpo.setField(fieldName, fieldValueSource.getFieldShort(fieldName));
            break;
          case BYTE:
            gpo.setField(fieldName, fieldValueSource.getFieldShort(fieldName));
            break;
          case OBJECT:
            gpo.setFieldObject(fieldName, fieldValueSource.getFieldObject(fieldName));
            break;
          default:
            throw new RuntimeException("Unhandled type: " + type);
        }
      }
    }
    return gpo;

  }

  public String getEventSchema()
  {
    return eventSchema;
  }

  public void setEventSchema(String eventSchema)
  {
    this.eventSchema = eventSchema;
  }

  public Map<MutablePair<String, Type>, MutablePair<String, Type>> getKeyValueMap()
  {
    return keyValueMap;
  }

  public void setKeyValueMap(Map<MutablePair<String, Type>, MutablePair<String, Type>> keyValueMap)
  {
    this.keyValueMap = keyValueMap;
  }

  //the staticFields depends on the schema, create the staticFields here and let the client populate the value.
  //this method should be called after set the keyValueMap
  public GPOMutable getStaticFields()
  {
    if(staticFields==null)
    {
      synchronized(this)
      {
        if(staticFields==null)
        {
          FieldsDescriptor fd = getFieldsDescriptor();
          if(fd == null)
            throw new RuntimeException("Please setKeyValueMap() first.");
          staticFields = new GPOMutable(fd);
        }
      }
    }
    return staticFields;
  }

}
