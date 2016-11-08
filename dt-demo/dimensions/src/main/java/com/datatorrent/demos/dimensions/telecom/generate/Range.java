/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

public class Range<T>
{
  public final T from;
  public final T to;

  public Range(T from, T to)
  {
    this.from = from;
    this.to = to;
  }
}
