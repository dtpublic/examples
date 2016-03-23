/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.model;

import java.util.Collection;
import java.util.Collections;

/**
 * The information of the customer.
 * We don't care about information such as customer name etc.
 * only care about the MSISDN, IMSI and IMEI etc. We identify customer by IMSI.
 * 
 * @author bright
 *
 */
public class CustomerInfo {
  public final String imsi;
  public final String msisdn;
  public final Collection<String> imeis;   //one imsi can map to multiple device
  
  //used only by reflection
  protected CustomerInfo()
  {
    imsi = "";
    msisdn = "";
    imeis = Collections.emptyList();
  }
  
  public CustomerInfo(String imsi, String msisdn, Collection<String> imeis)
  {
    this.imsi = imsi;
    this.msisdn = msisdn;
    this.imeis = Collections.unmodifiableCollection(imeis);
  }
  
}
