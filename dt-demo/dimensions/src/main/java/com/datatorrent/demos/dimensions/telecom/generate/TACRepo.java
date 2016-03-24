/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Map;
import java.util.Random;

import com.google.common.collect.Maps;

import com.datatorrent.demos.dimensions.telecom.model.TACInfo;

/**
 * 
 * @author bright
 * TAC: https://en.wikipedia.org/wiki/Type_Allocation_Code
TAC Manufacturer  Model Internal Model Number
01124500  Apple iPhone  
01130000  Apple iPhone  MA712LL
01136400  Apple iPhone  
01154600  Apple iPhone  MB384LL
01161200  Apple iPhone 3G 
01174400  Apple iPhone 3G MB496RS
01180800  Apple iPhone 3G MB704LL
01181200  Apple iPhone 3G MB496B
01193400  Apple iPhone 3G 
01194800  Apple iPhone 3GS  
01215800  Apple iPhone 3GS  
01215900  Apple iPhone 3GS  MC131B
01216100  Apple iPhone 3GS  
01226800  Apple iPhone 3GS  
01233600  Apple iPhone 4  MC608LL
01233700  Apple iPhone 4  MC603B
01233800  Apple iPhone 4  MC610LL
01241700  Apple iPhone 4  
01243000  Apple iPhone 4  MC603KS
01253600  Apple iPhone 4  MC610LL/A
01254200  Apple iPhone 4  
01300600  Apple iPhone 4S MD260C
01326300  Apple iPhone 4  MD198HN/A
01332700  Apple iPhone 5  MD642C
01388300  Apple iPhone 5S ME297C/A

35089080  Nokia 3410  NHM-2NX
35099480  Nokia 3410  NHM-2NX
35148420  Nokia 3410  NHM-2NX
35148820  Nokia 6310i NPL-1
35151304  Nokia E72-1 RM-530
35154900  Nokia 6310i NPL-1
35171005  Sony Ericsson Xperia S  
35174605  Google  Galaxy Nexus  Samsung GT-I9250, Samsung GT-I9250TSGGEN
35191405  Motorola  Defy Mini 
35226005  Samsung Galaxy SIII 
35238402  Sony Ericsson K770i 
35274901  Nokia 6233  
35291402  Nokia 6210 Navigator  
35316004  ZTE Blade 

35316605  Samsung Galaxy S3 GT-I9300
35332705  Samsung Galaxy SII  GT-I9100
35328504  Samsung Galaxy S  GT-I9000
35351200  Motorola  V300  
35357800  Samsung SGH-A800  
35376800  Nokia 6230  
35391805  Google  Nexus 4 LG E960
35405600  Wavecom M1306B  
35421803  Nokia 5310  RM-303
35433004  Nokia C5-00 RM-645
35450502  GlobeTrotter  HSDPA Modem 
35511405  Sony Ericsson Xperia U  
35524803  Nokia 2330C-2 RM-512
35566600  Nokia 6230  
35569500  Nokia 1100  
35679404  Samsung Galaxy Mini GT-S5570
35685702  Nokia 6300  
35693803  Nokia N900  
35694603  Nokia 2700  
35699601  Nokia N95 
35700804  Nokia C1  
35714904  Huawei  E398U-15 LTE Stick  
35733104  Samsung Galaxy Gio  
35739804  Nokia N8  
35744105  Samsung Galaxy S4 GT-I9505
35788104  Nokia N950  
35803106  HTC HTC One M8s 
35824005  Google  Nexus 5 LG D820/D821
35828103  Nokia 6303C 
35836800  Nokia 6230i 
35837501  XDA Orbit 2 
35837800  Nokia N6030 RM-74
35850000  Nokia Lumia 720 
35851004  Sony Ericsson Xperia Active 
35853704  Samsung Galaxy SII  
35869205  Apple iPhone 5S MF353TA/A
35876105  Apple iPhone 5S A1457
35896704  HTC Desire S  
35902803  HTC Wildfire  
35909205  Samsung Galaxy Note III SM-N9000, SM-N9005, SM-N900
35918804  HTC One X 
35920605  Nokia Lumia 625 
35929005  Motorola  Moto G  XT1039
35933005  OROD  6468  
35935003  Nokia 2720A-2 RM-519
35972100  Lobster 544 
35974101  GlobeTrotter  HSDPA Modem 
35979504  Samsung Galaxy Note 
86107402  Quectel Queclink GV200  
86217001  Quectel Queclink GV200  
86723902  ZTE Corporation Rook from EE, Orange Dive 30, Blade A410  
86813001  Jiayu G3S JY-G3
 */
public class TACRepo {
  private static final Random random = new Random();
  private Map<Integer, TACInfo> repo = Maps.newHashMap();
  private int[] keyArray;
  
  private static TACRepo instance = null;
  
  private TACRepo(){}
  
  public static TACRepo instance()
  {
    if(instance == null)
    {
      synchronized(TACRepo.class)
      {
        if(instance == null)
        {
          instance = new TACRepo();
          instance.init();
        }
      }
    }
    return instance;
  }
  
  protected void init()
  {
    repo.clear();
    //the following should be "iPhone", change to iPhone6 just in order to get more download data
    addNewTacInfo(1124500, "Apple", "iPhone 6");
    addNewTacInfo(1130000, "Apple", "iPhone 6");
    addNewTacInfo(1136400, "Apple", "iPhone 6");
    addNewTacInfo(1161200, "Apple", "iPhone 6");
    addNewTacInfo(1174400, "Apple", "iPhone 6");
    addNewTacInfo(1180800, "Apple", "iPhone 6");
    addNewTacInfo(1181200, "Apple", "iPhone 6");
    addNewTacInfo(1193400, "Apple", "iPhone 6");
    addNewTacInfo(1194800, "Apple", "iPhone 6");
    addNewTacInfo(1215800, "Apple", "iPhone 6");
    addNewTacInfo(1215900, "Apple", "iPhone 6");
    addNewTacInfo(1216100, "Apple", "iPhone 6");
    addNewTacInfo(1226800, "Apple", "iPhone 6");
    
    //following should be "iPhone 4", change to "iPhone 5S" just for more down load data.
    addNewTacInfo(1233600, "Apple", "iPhone 5S");
    addNewTacInfo(1233700, "Apple", "iPhone 5S");
    addNewTacInfo(1233800, "Apple", "iPhone 5S");
    addNewTacInfo(1241700, "Apple", "iPhone 5S");
    addNewTacInfo(1243000, "Apple", "iPhone 5S");
    addNewTacInfo(1253600, "Apple", "iPhone 5S");
    addNewTacInfo(1254200, "Apple", "iPhone 5S");
    
    addNewTacInfo(1300600, "Apple", "iPhone 4S");
    addNewTacInfo(1326300, "Apple", "iPhone 4");
    addNewTacInfo(1332700, "Apple", "iPhone 5");
    addNewTacInfo(1388300, "Apple", "iPhone 5S");
    
    addNewTacInfo(35089080, "Nokia", "3410");
    addNewTacInfo(35099480, "Nokia", "3410" );
    addNewTacInfo(35148420, "Nokia", "3410");
    addNewTacInfo(35148820, "Nokia", "6310i");
    addNewTacInfo(35151304, "Nokia", "E72-1");
    addNewTacInfo(35154900, "Nokia", "6310i");
    addNewTacInfo(35171005, "Sony Ericsson", "Xperia S");
    addNewTacInfo(35174605, "Google  Galaxy", "Nexus");
    addNewTacInfo(35191405, "Motorola", "Defy Mini" );
    addNewTacInfo(35226005, "Samsung", "Galaxy SIII");
    addNewTacInfo(35238402, "Sony Ericsson", "K770i");
    addNewTacInfo(35274901, "Nokia", "6233");
    addNewTacInfo(35291402, "Nokia", "6210 Navigator");
    addNewTacInfo(35316004, "ZTE", "Blade");
    
    //add others later is required 
    
    //cache keys
    keyArray = new int[repo.size()];
    int index = 0;
    for(int key : repo.keySet() )
      keyArray[index++] = key;
    
  }
  
  protected TACInfo addNewTacInfo(int tac, String manufacturer, String model)
  {
    TACInfo info = repo.get(tac);
    if(info == null)
    {
      info = new TACInfo(tac, manufacturer, model);
      repo.put(tac, info);
    }
    return info;
  }
  
  public int getRecordSize()
  {
    return repo.size();
  }
  
  public TACInfo getTacInfo(int tac)
  {
    return repo.get(tac);
  }
  
  public TACInfo getTacInfo(String tac)
  {
    return repo.get(Integer.valueOf(tac));
  }
  
  public TACInfo getTacInfoByImei(String imei)
  {
    return getTacInfo(imei.substring(0, 8));
  }
  
  public int getTacByIndex(int index)
  {
    return keyArray[index];
  }
  
  public int getRandomTac()
  {
    return getTacByIndex( random.nextInt(getRecordSize()) );
  }
  
  public TACInfo getRandomTacInfo()
  {
    return repo.get(getRandomTac());
  }
}
