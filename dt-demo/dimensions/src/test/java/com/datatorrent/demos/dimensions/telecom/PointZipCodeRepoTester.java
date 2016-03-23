/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom;

import java.util.Map;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * test cases
"95008", "CA", " 37.278843", "-121.95446", "Campbell", "California"
"95010", "CA", " 36.976100", "-121.95316", "Capitola", "California"
"95012", "CA", " 36.768856", "-121.75105", "Castroville", "California"
"95014", "CA", " 37.317909", "-122.04779", "Monte Vista", "California"
"95017", "CA", " 37.085986", "-122.22638", "Davenport", "California"
"95018", "CA", " 37.057708", "-122.05967", "Felton", "California"
"95019", "CA", " 36.935552", "-121.77972", "Freedom", "California"
"95020", "CA", " 37.016943", "-121.56581", "Gilroy", "California"
"95023", "CA", " 36.862243", "-121.38006", "Hollister", "California"
"95030", "CA", " 37.228594", "-121.98396", "Monte Sereno", "California"
"95032", "CA", " 37.241193", "-121.95340", "Los Gatos", "California"
"95033", "CA", " 37.160012", "-121.98810", "Los Gatos", "California"
"95035", "CA", " 37.436451", "-121.89438", "Milpitas", "California"
"95037", "CA", " 37.137595", "-121.66211", "Morgan Hill", "California"

 */
import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo;
import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo.Point;
import com.google.common.collect.Maps;

public class PointZipCodeRepoTester
{
  private Map<Point, Integer> pointToZipMap = Maps.newHashMap();

  @Before
  public void setUp()
  {
    pointToZipMap.put(new Point(37.2788f, -121.9544f), 95008);
    pointToZipMap.put(new Point(37.2786f, -121.9542f), 95008);

    //95018", "CA", " 37.057708", "-122.05967
    pointToZipMap.put(new Point(37.0577f, -122.0596f), 95018);
    pointToZipMap.put(new Point(37.0580f, -122.0599f), 95018);

    //pointToZipMap.put(new Point(, ),   ); 
  }

  @Test
  public void test()
  {
    LocationRepo repo = LocationRepo.instance();
    for (Map.Entry<Point, Integer> entry : pointToZipMap.entrySet()) {
      int actual = repo.getCloseLocationInfo(entry.getKey()).zipCode;
      Assert.assertTrue(String.format("Point: lan=%d, lon=%d; Zip: expected: %d; actual: %d", entry.getKey().scaledLat,
          entry.getKey().scaledLon, entry.getValue(), actual), entry.getValue() == actual);
    }
  }
}
