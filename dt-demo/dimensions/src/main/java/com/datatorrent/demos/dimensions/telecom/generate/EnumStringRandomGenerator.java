/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Random;

public class EnumStringRandomGenerator implements Generator<String>
{
  protected static final Random random = new Random();
  protected String[] candidates;

  public EnumStringRandomGenerator()
  {
  }

  public EnumStringRandomGenerator(String[] candidates)
  {
    if (candidates == null || candidates.length == 0) {
      throw new IllegalArgumentException("candidates can't null or empty.");
    }
    this.candidates = candidates;
  }

  public String next()
  {
    if (candidates.length == 1) {
      return candidates[0];
    }
    return candidates[random.nextInt(candidates.length)];
  }
}
