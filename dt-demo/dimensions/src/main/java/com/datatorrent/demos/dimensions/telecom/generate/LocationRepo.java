/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.demos.dimensions.telecom.model.ZipCodeHelper;

/**
 * This class keep the map between location(lan, lon) and zipcode. for
 * simplicity, we treat the area of zipcode is a circle. and this class keep the
 * middle point of the circle of the area. If a point most close to the middle
 * point of the circle, we treat it as in this area
 * 
 * @author bright
 *
 */
public class LocationRepo
{
  private static final transient Logger logger = LoggerFactory.getLogger(LocationRepo.class);

  private final String LOCATION_ZIPS_FILE = "usLocationToZips.csv";

  public static class Point
  {
    public static final int SCALAR = 10000;
    //instead of use float, use int to increase performance
    public final int scaledLat;
    public final int scaledLon;

    protected Point()
    {
      scaledLat = 0;
      scaledLon = 0;
    }

    public Point(int scaledLat, int scaledLon)
    {
      this.scaledLat = scaledLat;
      this.scaledLon = scaledLon;
    }

    public Point(float lat, float lon)
    {
      this.scaledLat = (int)(lat * SCALAR);
      this.scaledLon = (int)(lon * SCALAR);
    }

    public float getLat()
    {
      float lat = scaledLat;
      return lat / SCALAR;
    }

    public float getLon()
    {
      float lon = scaledLon;
      return lon / SCALAR;
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + scaledLat;
      result = prime * result + scaledLon;
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      Point other = (Point)obj;
      if (scaledLat != other.scaledLat) {
        return false;
      }
      if (scaledLon != other.scaledLon) {
        return false;
      }
      return true;
    }
  }

  public static class LocationInfo
  {
    public LocationInfo(Point point, int zipCode, String stateCode, String state, String city, float lat, float lon)
    {
      this.point = point;
      this.zipCode = zipCode;
      this.stateCode = stateCode;
      this.state = state;
      this.city = city;
      this.lat = lat;
      this.lon = lon;
    }

    public final Point point;
    public final int zipCode;
    public final String state; //"California"
    public final String stateCode; //"CA"
    public final String city;
    public final float lat;
    public final float lon;

    public String getZipCodeAsString()
    {
      return ZipCodeHelper.usZipCodeHelper.toString(zipCode);
    }
  }

  private static LocationRepo instance = null;

  private LocationRepo()
  {
  }

  public static LocationRepo instance()
  {
    if (instance == null) {
      synchronized (LocationRepo.class) {
        if (instance == null) {
          instance = new LocationRepo();
          instance.load();
        }
      }
    }

    return instance;
  }

  protected SortedMap<Point, LocationInfo> pointToLocationInfo = Maps.newTreeMap(new Comparator<Point>()
  {

    @Override
    public int compare(Point l1, Point l2)
    {
      if (l1.scaledLat < l2.scaledLat) {
        return -1;
      } else if (l1.scaledLat > l2.scaledLat) {
        return 1;
      } else if (l1.scaledLon < l2.scaledLon) {
        return -1;
      } else if (l1.scaledLon > l2.scaledLon) {
        return 1;
      }
      return 0;
    }

  });

  protected Map<Integer, LocationInfo> zipToLocationInfo = Maps.newHashMap();

  protected Random random = new Random();
  protected SortedMap<Integer, int[]> lanToLons = Maps.newTreeMap();
  protected int[] sortedLans;
  protected int[] zipCodes;
  protected Point[] points;

  /**
   * load from csv file
   * 
   * @throws IOException
   */
  protected void load()
  {
    logger.info("instance:{}; PointZipCodeRepo.load()", System.identityHashCode(this));

    InputStream is = null;
    BufferedReader br = null;
    try {
      is = this.getClass().getClassLoader().getResourceAsStream(LOCATION_ZIPS_FILE);
      br = new BufferedReader(new InputStreamReader(is));
      while (true) {
        String line = br.readLine();
        if (line == null) {
          break;
        }
        
        addLocationInfo(line);
      }
      logger.info("load(): {} of pointToZip entries are loaded.", pointToLocationInfo.size());

      //generate LanToLons
      Map<Integer, List<Integer>> lanToLonList = Maps.newHashMap();
      Set<Point> pointSet = pointToLocationInfo.keySet();
      points = new Point[pointSet.size()];
      int index = 0;
      for (Point point : pointSet) {
        points[index++] = point;
        List<Integer> lons = lanToLonList.get(point.scaledLat);
        if (lons == null) {
          lons = Lists.newArrayList(point.scaledLon);
          lanToLonList.put(point.scaledLat, lons);
        } else {
          lons.add(point.scaledLon);
        }
      }
      logger.info("load(): {} of points are loaded.", points.length);

      List<Integer> lans = Lists.newArrayList(lanToLonList.keySet());
      Collections.sort(lans);

      sortedLans = toArray(lans);

      //sort all the lons and convert Lon list to array
      for (Map.Entry<Integer, List<Integer>> entry : lanToLonList.entrySet()) {
        Collections.sort(entry.getValue());
        lanToLons.put(entry.getKey(), toArray(entry.getValue()));
      }

      //get zip codes
      Set<Integer> zipCodeSet = zipToLocationInfo.keySet();

      zipCodes = new int[zipCodeSet.size()];
      index = 0;
      for (Integer zipCode : zipCodeSet) {
        zipCodes[index++] = zipCode;
      }
      logger.info("load(): {} of zipcode are loaded.", zipCodes.length);

    } catch (FileNotFoundException e) {
      logger.error("Exception in load()", e);
    } catch (IOException e) {
      logger.error("Exception in load()", e);
    } finally {
      if (br != null) {
        IOUtils.closeQuietly(br);
      }
      if (is != null) {
        IOUtils.closeQuietly(is);
      }
    }
  }

  protected static int[] toArray(List<Integer> list)
  {
    int[] array = new int[list.size()];
    int index = 0;
    for (int value : list) {
      array[index++] = value;
    }
    return array;
  }

  /**
   * csv format. example: #”zip code", "state abbreviation", "latitude", "
   * longitude", "city", "state" "35004", "AL", " 33.606379", " -86.50249",
   * "Moody", "Alabama"
   * 
   * @param line
   */
  public void addLocationInfo(String line)
  {
    line = line.trim();
    if (line.isEmpty() || line.startsWith("#")) {
      return;
    }
    String[] items = line.split(",");
    try {
      addLocaationInfo(Float.valueOf(trimItem(items[2])), Float.valueOf(trimItem(items[3])),
          ZipCodeHelper.usZipCodeHelper.toInt(trimItem(items[0])), trimItem(items[1]), trimItem(items[5]),
          trimItem(items[4]));
    } catch (Exception e) {
      logger.warn("Invalid line: {}", line);
    }
  }

  /**
   * get rid possible space, quote etc
   * 
   * @param item
   * @return
   */
  public static String trimItem(String item)
  {
    if (item == null || item.isEmpty()) {
      return item;
    }
    item = item.trim();
    if (item.startsWith("\'") || item.startsWith("\"")) {
      item = item.substring(1);
    }
    if (item.endsWith("\'") || item.endsWith("\"")) {
      item = item.substring(0, item.length() - 1);
    }
    return item.trim();
  }

  public void addLocaationInfo(float lat, float lon, int zip, String stateCode, String state, String city)
  {
    Point point = new Point(lat, lon);
    LocationInfo li = new LocationInfo(point, zip, stateCode, state, city, lat, lon);
    pointToLocationInfo.put(point, li);
    zipToLocationInfo.put(zip, li);
  }

  public LocationInfo getLocationInfoByZip(String zip)
  {
    int zipCode = Integer.valueOf(zip);
    return zipToLocationInfo.get(zipCode);
  }

  public LocationInfo getCloseLocationInfo(Point point)
  {
    return getCloseLocationInfo(point.getLat(), point.getLon());
  }

  /**
   * get zip by lan and lon
   * 
   * @param lan
   * @param lon
   * @return
   */
  public LocationInfo getCloseLocationInfo(float lan, float lon)
  {
    int iLan = (int)(lan * Point.SCALAR);
    int iLon = (int)(lon * Point.SCALAR);

    return getLocationInfoByPoint(getClosePointByScaledPoint(iLan, iLon));
  }

  /**
   * @param pint
   *          The point should be exact same as the point of one record.
   * @return
   */
  public LocationInfo getLocationInfoByPoint(Point pint)
  {
    return pointToLocationInfo.get(pint);
  }

  private static final int[] stepSizes = new int[] {1, -1};

  public Point getClosePointByScaledPoint(int lan, int lon)
  {
    final int lanIndex = getIndexOfMostCloseToLan(lan);
    Long minDistanceSquare = Long.MAX_VALUE;
    int candidateLan = 0;
    int candidateLon = 0;
    for (int stepSize : stepSizes) {
      if (minDistanceSquare == 0) {
        break;
      }
      for (int index = lanIndex; minDistanceSquare > 0 && index < sortedLans.length && index >= 0; index += stepSize) {
        final int closeLon = findMostCloseLon(sortedLans[index], lon);
        long distanceSquare = distanceSquare(lan, lon, sortedLans[index], closeLon);
        if (distanceSquare < minDistanceSquare) {
          minDistanceSquare = distanceSquare;
          candidateLan = sortedLans[index];
          candidateLon = closeLon;
        }
        if (distanceSquare(lan, sortedLans[index]) >= minDistanceSquare) {
          break;
        }
      }
    }
    return getPoint(candidateLan, candidateLon);
  }

  //distance of one dimension
  public static long distanceSquare(int value, int value1)
  {
    long diffValue = value1 - value;
    return diffValue * diffValue;
  }

  public static long distanceSquare(int lan, int lon, int lan1, int lon1)
  {
    long diffLan = lan1 - lan;
    long diffLon = lon1 - lon;
    return diffLan * diffLan + diffLon * diffLon;
  }

  protected Point getPoint(int lan, int lon)
  {
    return new Point(lan, lon);
  }

  public int getIndexOfMostCloseToLan(int matchLan)
  {
    return getIndexOfMostCloseToValue(sortedLans, matchLan);
  }

  public int findMostCloseLon(int lan, int matchLon)
  {
    return lanToLons.get(lan)[getIndexOfMostCloseToValue(lanToLons.get(lan), matchLon)];
  }

  public int getIndexOfMostCloseToLon(int lan, int matchLon)
  {
    return getIndexOfMostCloseToValue(lanToLons.get(lan), matchLon);
  }

  public static int getIndexOfMostCloseToValue(int[] values, int matchValue)
  {
    return getIndexOfMostCloseToValue(values, matchValue, 0, values.length - 1);
  }

  public static int getIndexOfMostCloseToValue(int[] values, int matchValue, int start, int end)
  {
    if (values == null || values.length == 0) {
      throw new IllegalArgumentException("Input values should not null or empty.");
    }

    if (start == end) {
      return start;
    }
    if (start + 1 == end) {
      return Math.abs(values[start] - matchValue) < Math.abs(values[end] - matchValue) ? start : end;
    }

    if (matchValue <= values[start]) {
      return start;
    }
    if (matchValue >= values[end]) {
      return end;
    }

    int middleIndex = (start + end) / 2;
    if (matchValue == values[middleIndex]) {
      return middleIndex;
    }
    if (matchValue > values[middleIndex]) {
      return getIndexOfMostCloseToValue(values, matchValue, middleIndex, end);
    } else {
      return getIndexOfMostCloseToValue(values, matchValue, start, middleIndex);
    }
  }

  public String getRandomZipCode()
  {
    if (zipCodes == null) {
      throw new RuntimeException("load() first.");
    }
    return String.valueOf(zipCodes[random.nextInt(zipCodes.length)]);
  }

  public Point getRandomPoint()
  {
    return points[random.nextInt(points.length)];
  }
}
