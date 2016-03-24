/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.appdata.snapshot.AbstractAppDataSnapshotServer;

public abstract class AppDataConfigurableSnapshotServer<E> extends AbstractAppDataSnapshotServer<E>
{
  //map of field name to value
  private Map<String, Object> staticFieldsInfo = Maps.newHashMap(); 

  //fieldName ==> fieldType
  private Map<String, Type> fieldInfoMap;
  
  protected transient FieldsDescriptor fieldsDescriptor;
  protected transient GPOMutable staticFields;
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    buildStaticFields();
  }
  
  protected void buildStaticFields()
  {
    if(staticFieldsInfo == null)
      return;
    
    FieldsDescriptor fd = getFieldsDescriptor();
    if(fd == null)
      throw new RuntimeException("Please setKeyValueMap() first.");
    staticFields = new GPOMutable(fd);
    
    for(Map.Entry<String, Object> entry :staticFieldsInfo.entrySet())
    {
      if(entry.getValue() instanceof Long)
        staticFields.setField(entry.getKey(), (Long)entry.getValue());
      else if(entry.getValue() instanceof String)
        staticFields.setField(entry.getKey(), (String)entry.getValue());
      else
        throw new RuntimeException("Not supported value type: " + entry.getValue().getClass());
    }
  }
  
  public GPOMutable convert(E row)
  {
    GPOMutable gpo;
    if(staticFields != null)
    {
      gpo = new GPOMutable(staticFields);
    }
    else
      gpo = new GPOMutable(getFieldsDescriptor());
    
    convertTo(row, gpo);
    return gpo;
  }
  
  protected abstract void convertTo(E row, GPOMutable gpo);

  protected FieldsDescriptor getFieldsDescriptor()
  {
    if (fieldsDescriptor == null) {
      fieldsDescriptor = new FieldsDescriptor(fieldInfoMap);
    }
    return fieldsDescriptor;
  }

  public Map<String, Type> getFieldInfoMap()
  {
    return fieldInfoMap;
  }

  public void setFieldInfoMap(Map<String, Type> fieldInfoMap)
  {
    this.fieldInfoMap = fieldInfoMap;
  }

  public Map<String, Object> getStaticFieldsInfo()
  {
    return staticFieldsInfo;
  }

  public void setStaticFieldsInfo(Map<String, Object> staticFieldsInfo)
  {
    this.staticFieldsInfo = staticFieldsInfo;
  }

  public void addStaticFieldInfo(String fieldName, long value)
  {
    staticFieldsInfo.put(fieldName, value);
  }
  public void addStaticFieldInfo(String fieldName, String value)
  {
    staticFieldsInfo.put(fieldName, value);
  }
}
