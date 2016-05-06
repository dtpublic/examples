/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

import java.util.Calendar;

public class CustomerService
{
  public static final String delimiter = ";";

  public static enum IssueType
  {
    DeviceUpgrade, CallQuality, DeviceQuality, Billing, NetworkCoverage, Roaming
  }

  public final String imsi;
  public final String isdn;
  public final String imei;
  public final int totalDuration;
  public final int wait;
  public final String zipCode;
  public final IssueType issueType;
  public final boolean satisfied;
  public final long time = Calendar.getInstance().getTimeInMillis();

  protected CustomerService()
  {
    imsi = "";
    isdn = "";
    imei = "";
    totalDuration = 0;
    wait = 0;
    zipCode = "";
    issueType = null;
    satisfied = false;
  }

  public CustomerService(String imsi, String isdn, String imei, int totalDuration, int wait, String zipCode,
      IssueType issueType, boolean satisfied)
  {
    this.imsi = imsi;
    this.isdn = isdn;
    this.imei = imei;
    this.totalDuration = totalDuration;
    this.wait = wait;
    this.zipCode = zipCode;
    this.issueType = issueType;
    this.satisfied = satisfied;
  }

  public CustomerService(CustomerService other)
  {
    this(other.imsi, other.isdn, other.imei, other.totalDuration, other.wait, other.zipCode, other.issueType,
        other.satisfied);
  }

  @Override
  public String toString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(imsi).append(delimiter);
    sb.append(isdn).append(delimiter);
    sb.append(imei).append(delimiter);
    sb.append(totalDuration).append(delimiter);
    sb.append(wait).append(delimiter);
    sb.append(zipCode).append(delimiter);
    sb.append(issueType).append(delimiter);
    sb.append(satisfied).append(delimiter);

    return sb.toString();
  }

  public int getServiceCallCount()
  {
    return 1;
  }

  public int getWait()
  {
    return wait;
  }

  public String getZipCode()
  {
    return zipCode;
  }

  public long getTime()
  {
    return time;
  }

  public String getIssueType()
  {
    return issueType.name();
  }

  //return 100 is satisfied, else 0
  public long getSatisfaction()
  {
    return satisfied ? 100 : 0;
  }

  /**
   * use 2 letter of zip as the region
   * 
   * @return
   */
  public String getRegionZip2()
  {
    return getZipSubString(2);
  }

  /**
   * use 2 letter of zip as the region
   * 
   * @return
   */
  public String getRegionZip3()
  {
    return getZipSubString(3);
  }

  protected String getZipSubString(int length)
  {
    if (length > zipCode.length()) {
      throw new IllegalArgumentException(
          "The length of the zipCode ( " + zipCode.length() + ") is less than begin length: (" + length + ").");
    }
    return zipCode.substring(0, length);
  }
}
