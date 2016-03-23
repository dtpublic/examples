/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

public class ZipCodeHelper
{
  public static ZipCodeHelper usZipCodeHelper = new ZipCodeHelper(5);

  protected static final String ZEROS = "0000000000";

  protected ZipCodeHelper(int zipCodeLength)
  {
    this.zipCodeLength = zipCodeLength;
  }

  private int zipCodeLength;

  public int toInt(String zip)
  {
    return Integer.parseInt(zip);
  }

  public String toString(int zipCode)
  {
    String zip = String.valueOf(zipCode);
    if (zip.length() == zipCodeLength)
      return zip;
    if (zip.length() < zipCodeLength)
      return ZEROS.substring(0, zipCodeLength - zip.length()) + zip;
    throw new IllegalArgumentException(
        "The length of zip (" + zipCode + ") is large than expected length (" + zipCodeLength + ")");
  }

  public boolean isZip(String str)
  {
    if (str == null || str.length() != zipCodeLength)
      return false;
    try {
      Integer.parseInt(str);
    } catch (Exception e) {
      return false;
    }
    return true;
  }
}
