/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

public class MsisdnGenerator extends StringComposeGenerator{
  @SuppressWarnings("unchecked")
  public MsisdnGenerator()
  {
    super( new EnumStringRandomGenerator(new String[]{"01"}), 
        new EnumStringRandomGenerator(new String[]{"408", "650", "510", "415", "925", "707"}),
        new FixLengthStringRandomGenerator(CharRandomGenerator.digitCharGenerator, 7) );
  }

}
