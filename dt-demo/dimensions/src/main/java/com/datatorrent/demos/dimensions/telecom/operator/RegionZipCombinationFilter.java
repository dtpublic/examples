/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.datatorrent.contrib.dimensions.CombinationFilter;

public class RegionZipCombinationFilter implements CombinationFilter, Serializable
{
  private static final long serialVersionUID = 1113431090576498579L;

  private static final Logger logger = LoggerFactory.getLogger(RegionZipCombinationFilter.class);
  
  public final static String KEY_REGION = "region";
  public final static String KEY_ZIP = "zipcode";
  
  @SuppressWarnings("unchecked")
  @Override
  public Map<String, Set<Object>> filter(Map<String, Set<Object>> keyToValues)
  {
    @SuppressWarnings("rawtypes")
    Set<String> regions = (Set)keyToValues.get(KEY_REGION);
    Set<Object> zips = keyToValues.remove(KEY_ZIP);
    
    //use new set instead of remove item for old set as there should have much more invalid zips than valid
    Set<Object> cleanedZips = Sets.newHashSet();
    for(Object zip : zips)
    {
      String sZip = (String)zip;
      if(isValidZip(regions, sZip))
        cleanedZips.add(zip);
    }
    keyToValues.put(KEY_ZIP, cleanedZips);
    
    logger.info("Original zip size: {}; cleanuped zip size: {}", zips.size(), cleanedZips.size()); 
    return keyToValues;
  }

  /**
   * the region is the first digits of zip
   * @param regions
   * @param zip
   * @return
   */
  public static boolean isValidZip(Set<String> regions, String zip)
  {
    for(String region : regions)
    {
      if(zip.startsWith(region))
        return true;
    }
    return false;
  }
}
