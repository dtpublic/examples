/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.contrib.dimensions.CombinationValidator;

public class RegionZipCombinationValidator implements CombinationValidator<String, String>, Serializable
{
  private static final long serialVersionUID = 1566721667582387976L;

  private static final Logger logger = LoggerFactory.getLogger(RegionZipCombinationValidator.class);

  public static final String KEY_REGION = "region";
  public static final String KEY_ZIP = "zipcode";

  //make sure region is before zip
  @Override
  public List<String> orderKeys(List<String> keys)
  {
    int indexRegion = -1;
    int indexZip = -1;
    for (int index = 0; index < keys.size(); ++index) {
      if (indexRegion >= 0 && indexZip >= 0) {
        break;
      }
      String key = keys.get(index);
      if (KEY_REGION.equals(key)) {
        if (indexZip < 0) {
          return keys;
        }
        indexRegion = index;
        break; //both zip and region had found
      }
      if (KEY_ZIP.equals(key)) {
        //indexRegion must < 0
        indexZip = index;
      }
    }

    //indexRegion must > indexZip; switch
    keys.set(indexZip, KEY_REGION);
    keys.set(indexRegion, KEY_ZIP);
    return keys;
  }

  @Override
  public boolean isValid(Map<String, Set<String>> combinedKeyValues, String key, String value)
  {
    if (!KEY_ZIP.equals(key)) {
      return true;
    }
    if (value == null || value.length() != 5) {
      return false;
    }
    Set<String> regions = combinedKeyValues.get(KEY_REGION);

    return (regions != null && regions.contains(value.substring(0, 2)));
  }

}
