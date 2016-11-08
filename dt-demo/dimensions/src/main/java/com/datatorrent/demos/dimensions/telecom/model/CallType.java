/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

public enum CallType
{
  MOC, MTC, SMS_MO("SMS-MO"), SMS_MT("SMS-MT"), DATA;

  private String label;
  private static final String[] labels = {"MOC", "MTC", "SMS-MO", "SMS-MT", "DATA"};

  private CallType()
  {
    this.label = name();
  }

  private CallType(String label)
  {
    this.label = label;
  }

  public String label()
  {
    return this.label;
  }

  public static String[] labels()
  {
    return labels;
  }

  public static CallType labelOf(String label)
  {
    for (CallType ct : CallType.values()) {
      if (ct.label().equals(label)) {
        return ct;
      }
    }
    throw new IllegalArgumentException("Invalid CallType label: " + label);
  }
}
