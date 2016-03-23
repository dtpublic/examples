/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

public abstract class AbstractStringRandomGenerator implements Generator<String> {
  protected CharRandomGenerator charGenerator;
  
  @Override
  public String next()
  {
    if(charGenerator == null)
      throw new RuntimeException("Please set the char generator first.");
    final int stringLen = getStringLength();
    if(stringLen < 0 )
      throw new RuntimeException("The string lenght expect not less than zero.");
    if(stringLen == 0)
      return "";
    char[] chars = new char[stringLen];
    for(int index=0; index<stringLen; ++index)
      chars[index] = charGenerator.next();
    return new String(chars);
  }
  
  protected abstract int getStringLength();
}
