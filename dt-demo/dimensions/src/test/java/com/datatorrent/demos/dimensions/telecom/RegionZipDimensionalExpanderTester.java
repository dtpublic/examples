/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.datatorrent.contrib.dimensions.CombinationDimensionalExpander;
import com.datatorrent.contrib.dimensions.CombinationValidator;
import com.datatorrent.demos.dimensions.telecom.operator.RegionZipCombinationFilter;
import com.datatorrent.demos.dimensions.telecom.operator.RegionZipCombinationValidator;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;

public class RegionZipDimensionalExpanderTester
{
  public static final String KEY_REGION = "region";
  public static final String KEY_ZIP = "zipcode";

  protected Map<String, Set<Object>> seenEnumValues;
  protected RegionZipCombinationFilter filter = new RegionZipCombinationFilter();
  protected RegionZipCombinationValidator validator = new RegionZipCombinationValidator();
  protected Map<String, Type> fieldToType;

  @Before
  public void setup()
  {
    fieldToType = Maps.newHashMap();
    fieldToType.put(KEY_REGION, Type.STRING);
    fieldToType.put(KEY_ZIP, Type.STRING);

    seenEnumValues = Maps.newHashMap();
    Set<String> regions = Sets.newHashSet("91", "92", "93", "94");
    seenEnumValues.put(KEY_REGION, (Set)regions);
    Set<String> zips = Sets.newHashSet();
    for (String region : regions) {
      for (int i = 100; i < 1000; ++i) {
        zips.add(region + i);
      }
    }
    seenEnumValues.put(KEY_ZIP, (Set)zips);
  }

  @Test
  public void test()
  {
    CombinationDimensionalExpander expander = new CombinationDimensionalExpander((Map)seenEnumValues).withCombinationFilter(filter).withCombinationValidator((CombinationValidator)validator);

    final FieldsDescriptor fd = new FieldsDescriptor(fieldToType);
    Map<String, Set<Object>> keyToValues = Maps.newHashMap();
    keyToValues.put(KEY_REGION, (Set)Sets.newHashSet("91", "92", "93"));
    keyToValues.put(KEY_ZIP, Sets.newHashSet());

    List<GPOMutable> GPOs = expander.createGPOs(keyToValues, fd);
    Assert.assertTrue("Invalid size. expect 2700, actual " + GPOs.size(), GPOs.size() == 2700);
  }
}
