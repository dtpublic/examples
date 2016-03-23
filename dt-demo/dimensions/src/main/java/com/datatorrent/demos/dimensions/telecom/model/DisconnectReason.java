/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

import java.util.Random;

public enum DisconnectReason {
  NoResponse(9, "No Response"),
  CallComplete(10, "Call Complete"),
  CallDropped(11, "Call Dropped")
  ;
  
  private int code;
  private String label;
  
  private DisconnectReason(){}
  
  private DisconnectReason(int code, String label)
  {
    this.code = code;
    this.label = label;
  }

  public final int getCode() {
    return code;
  }

  public String getLabel() {
    return label;
  }
  
  private static Random random = new Random();
  public static DisconnectReason randomDisconnectReason()
  {
    final int size = DisconnectReason.values().length;
    return DisconnectReason.values()[random.nextInt(size)];
  }
  
  public static DisconnectReason fromCode(int code)
  {
    for(DisconnectReason dr : DisconnectReason.values())
    {
      if(dr.code == code)
        return dr;
    }
    throw new IllegalArgumentException("Invalid disconnect code: " + code);
  }
}
