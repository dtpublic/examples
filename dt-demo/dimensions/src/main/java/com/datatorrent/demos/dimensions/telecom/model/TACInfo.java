/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

public class TACInfo {
  //tac is 8 digits, can be treat as int
  public final int tac;
  public final String manufacturer;
  public final String model;
  
  public TACInfo(int tac, String manufacturer, String model)
  {
    this.tac = tac;
    this.manufacturer = manufacturer;
    this.model = model;
  }
  
  public String getTacAsString()
  {
    return String.format("%08d", tac);
  }
  
  @Override
  public boolean equals(Object obj)
  {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final TACInfo other = (TACInfo)obj;
    
    return this.tac == other.tac;
  }
  
  @Override
  public int hashCode()
  {
    return tac;
  }
}
