/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * keep all instances generated at runtime
 * 
 * There could different type of instances of this class.
 * The instances which was just cloned should have the same id.
 * The really new instances should have a different id from other.
 * 
 * LIMITATION: this class can only used for one JVM, typically for unit test
 *
 */
@SuppressWarnings("rawtypes")
public class DataWrapper<T>
{
  //the instances which created by kyro;
  private static List<DataWrapper> clonedInstances = Lists.newArrayList();
  
  private static Map<String, DataWrapper> idToInstance = Maps.newHashMap();
  
  private T data;
  private String id;
  
  //for kyro only or internal 
  private DataWrapper()
  {
    clonedInstances.add(this);
  }

  /**
   * must specify id for normal cases
   * @param id
   */
  public static DataWrapper getOrCreateInstanceOfId(String id)
  {
    if(id == null || id.isEmpty())
      throw new IllegalArgumentException("Input parameter id should NOT null or empty.");
    
    DataWrapper instance = idToInstance.get(id);
    if(instance == null)
    {
      synchronized(DataWrapper.class)
      {
        if(instance == null)
        {
          instance = new DataWrapper();
          instance.setId(id);
          idToInstance.put(id, instance);
        }
      }
    }
    return instance;
  }

  public static synchronized DataWrapper getInstancesOfId(String id)
  {
    return idToInstance.get(id);
  }
  
  public synchronized T getOrSetData(T data)
  {
    if(this.data == null)
    {
      this.data = data;
    }
    return this.data;
  }
  
  /*
   * There could be multiple instances of this class.
   * Sync the data to let each instances with same id share the same data instance
   * This method can be called in the setup()
   */
  public T syncData()
  {
    if(data == null)
      throw new IllegalArgumentException("Data should be already set when syncData().");
    
    for( DataWrapper clonedInstance : clonedInstances)
    {
      if(id.equals(clonedInstance.id))
        clonedInstance.data = data;
    }
    return data;
  }
  
  public String getId()
  {
    return id;
  }
  public void setId(String id)
  {
    this.id = id;
  }

  public void setData(T data)
  {
    this.data = data;
  }
  public T getData()
  {
    return this.data;
  }
}
