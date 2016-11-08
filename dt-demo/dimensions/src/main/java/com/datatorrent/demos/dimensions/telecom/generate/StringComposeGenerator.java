/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.List;

import com.google.common.collect.Lists;

public class StringComposeGenerator<T> implements Generator<String>
{
  private List<Generator<String>> generators;

  public StringComposeGenerator()
  {
  }

  @SafeVarargs
  public StringComposeGenerator(Generator<String>... generators)
  {
    if (generators == null || generators.length == 0) {
      return;
    }
    this.generators = Lists.newArrayList(generators);
  }

  @Override
  public String next()
  {
    StringBuilder sb = new StringBuilder();
    for (Generator<String> generator : generators) {
      sb.append(generator.next());
    }
    return sb.toString();
  }

  public List<Generator<String>> getGenerators()
  {
    return generators;
  }

  public void setGenerators(List<Generator<String>> generators)
  {
    this.generators = generators;
  }

}
