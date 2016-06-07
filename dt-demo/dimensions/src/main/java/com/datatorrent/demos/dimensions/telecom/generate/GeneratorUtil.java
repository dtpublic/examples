/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Calendar;

public class GeneratorUtil
{
  public static long TIME_2010 = getTime2010();
  
  private static long getTime2010() 
  {
    Calendar calendar2010 = Calendar.getInstance();
    calendar2010.set(2010, 1, 1, 0, 0, 0);
    return calendar2010.getTimeInMillis();
  }

  public static long getRecordId()
  {
    return (Calendar.getInstance().getTimeInMillis() - TIME_2010) * 1000;
  }
}
