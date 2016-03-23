/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;

import com.datatorrent.demos.dimensions.telecom.model.MNCInfo;
import com.datatorrent.demos.dimensions.telecom.model.MNCInfo.Carrier;

public class MNCRepo {
  
  //MCC to MNC: see https://en.wikipedia.org/wiki/List_of_mobile_network_codes_in_the_United_States
  private static final Random random = new Random();
  private static MNCRepo instance;
  
  //MCC+MNC => MNCInfo
  private Map<Integer, MNCInfo> repo = Maps.newHashMap();
  private int[] keyArray;

  
  private MNCRepo(){}
  
  public static MNCRepo instance()
  {
    if(instance == null)
    {
      synchronized(MNCRepo.class)
      {
        if(instance == null)
        {
          instance = new MNCRepo();
          instance.init();
        }
      }
    }
    return instance;
  } 
  
  protected void init()
  {
    repo.clear();
    addNewMncInfo(310, 12, Carrier.VZN);
//    addNewMncInfo(310, 13, "MobileTel");
//    addNewMncInfo(310, 15, "SouthernLINC");
//    addNewMncInfo(310, 16, "Cricket Communications");
//    addNewMncInfo(310, 20, "Union Telephone");
//    addNewMncInfo(310, 26, "T-Mobile USA");
//    addNewMncInfo(310, 30, "Centennial Communications");
//    addNewMncInfo(310, 32, "IT&E");
//    addNewMncInfo(310, 34, "Airpeak");
//    addNewMncInfo(310, 40, "Concho Wireless/ Commnet");
//    addNewMncInfo(310, 50, "Cingular Wireless");
//    addNewMncInfo(310, 60, "Consolidated Telecom");
    addNewMncInfo(310, 70, Carrier.ATT);
//    addNewMncInfo(310, 80, "Corr Wireless");
//    addNewMncInfo(310, 90, "Cricket Communications");
//    addNewMncInfo(310, 100, "Plateau Wireless");
//    addNewMncInfo(310,110, "IT&E");
    addNewMncInfo(310,120, Carrier.SPR);
//    addNewMncInfo(310,130, "Cellular One");
//    addNewMncInfo(310,140, "Pulse Mobile (GTA)");
//    addNewMncInfo(310,150, "Aio Wireless");
    addNewMncInfo(310,160,  Carrier.TMO);
    addNewMncInfo(310,170, Carrier.TMO);
//    addNewMncInfo(310,180, "West Central Wireless");
//    addNewMncInfo(310,190, "Alaska Telecom");

    //others
    
    //cache keys
    keyArray = new int[repo.size()];
    int index = 0;
    for(int key : repo.keySet() )
      keyArray[index++] = key;
    
  }
  
  public MNCInfo addNewMncInfo(int mcc, int mnc, Carrier carrier)
  {
    MNCInfo info = getMncInfo(mcc, mnc);
    if(info == null)
    {
      info = new MNCInfo(mcc, mnc, carrier);
      putMncInfo(mcc, mnc, info);
    }
    return info;
  }
  
  public MNCInfo getMncInfo(int mcc, int mnc)
  {
    return repo.get(getMccMnc(mcc, mnc));
  }
  
  public MNCInfo getMncInfo(String mccAndMnc)
  {
    return repo.get(Integer.valueOf(mccAndMnc));
  }
  
  public MNCInfo getMncInfoByImsi(String imsi)
  {
    return getMncInfo(imsi.substring(0, 6));
  }
  
  public void putMncInfo(int mcc, int mnc, MNCInfo info)
  {
    repo.put(getMccMnc(mcc, mnc), info);
  }
  
  public int getRecordSize()
  {
    return repo.size();
  }
  
  public int getRandomMccMnc()
  {
    return getMccMncByIndex(random.nextInt(getRecordSize()) );
  }
  
  public int getMccMncByIndex(int index)
  {
    return keyArray[index];
  }
  
  public MNCInfo getRandomMncInfo()
  {
    return repo.get(getRandomMccMnc());
  }

  public int getMccMnc(int mcc, int mnc)
  {
    //mnc is 3 digits
    return mcc*1000 + mnc;
  }
}
