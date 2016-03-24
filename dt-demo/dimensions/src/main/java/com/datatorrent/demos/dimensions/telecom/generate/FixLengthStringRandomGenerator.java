/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

public class FixLengthStringRandomGenerator extends AbstractStringRandomGenerator{
  protected int length;
  
  public FixLengthStringRandomGenerator(){}
  
  public FixLengthStringRandomGenerator(CharRandomGenerator charGenerator, int length)
  {
    if(length <= 0 )
      throw new IllegalArgumentException("The length should large than zero.");
    this.length = length;
    this.charGenerator = charGenerator;
  }
  
  @Override
  protected int getStringLength() {
    return length;
  }
}
