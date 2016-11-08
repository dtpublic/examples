/**
 * Copyright (c) 2016 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.generate;

import java.util.Calendar;
import java.util.Random;

import com.datatorrent.demos.dimensions.telecom.generate.LocationRepo.Point;
import com.datatorrent.demos.dimensions.telecom.model.CallDetailRecord;
import com.datatorrent.demos.dimensions.telecom.model.CallType;
import com.datatorrent.demos.dimensions.telecom.model.DisconnectReason;

/**
 * record example MSIDN; IMSI; IMEI;
 * PLAN;CALL_TYPE;CORRESP_TYPE;CORRESP_ISDN;DURATION;BYTES;DR; LAT; LONG;
 * TIME;DATE 068373748102;208100167682477;351905149071;PLAN1;MOC; CUST1;
 * 0612287077; 247; ; 10; 32.9546; -97.015; 12:07:12;01/01/2015
 * 068373748102;208100167682477;351905149071;PLAN1;MTC; CUST2; 0600000001; 300;
 * ; 10; 32.9546; -97.015; 12:15:09;01/01/2015
 * 068373748102;208100167682477;351905149071;PLAN1;SMS-MO; CUST1; 0613637193; 0;
 * ; ; 32.92748; -96.9595; 12:18:18;01/01/2015
 * 068373748102;208100167682477;351905149071;PLAN1;SMS-MT; CUST1; 0612899062; 0;
 * ; ; 32.9656; -96.8816; 12:21:07;01/01/2015
 * 065978198280;208100310191699;356008289837;PLAN3;MOC; CUST1; 0612283725; 90; ;
 * 11; 33.0154; -96.5501; 12:00:00;01/01/2015
 * 065978198280;208100310191699;356008289837;PLAN3;MOC; CUST1; 0613069656; 82; ;
 * 10; 33.07818; -96.7944; 12:02:27;01/01/2015
 * 065978198280;208100310191699;356008289837;PLAN3;DATA; CUST1; 0613481951; 0;
 * 150 ; 33.09851; -96.6374; 12:04:41;01/01/2015
 */
public class CallDetailRecordRandomGenerator implements Generator<CallDetailRecord>
{
  private static final Random random = new Random();

  private CharRandomGenerator digitCharGenerator = CharRandomGenerator.digitCharGenerator;
  private MsisdnGenerator msidnGenerator = new MsisdnGenerator();
  private ImsiGenerator imsiGenerator = new ImsiGenerator();
  private ImeiGenerator imeiGenerator = new ImeiGenerator();
  private EnumStringRandomGenerator planGenerator = new EnumStringRandomGenerator(
      new String[] {"PLAN1", "PLAN2", "PLAN3", "PLAN4"});
  private EnumStringRandomGenerator callTypeGenerator = new EnumStringRandomGenerator(CallType.labels());
  private EnumStringRandomGenerator correspTypeGenerator = new EnumStringRandomGenerator(
      new String[] {"CUST1", "CUST2", "CUST3"});
  private FixLengthStringRandomGenerator correspIsdnGenerator = new FixLengthStringRandomGenerator(digitCharGenerator,
      10);

  private boolean generateCustomerInfo = false;

  //initial as yesterday.
  //private transient volatile long recordTime = Calendar.getInstance().getTimeInMillis() - 24*60*60*1000;

  @Override
  public CallDetailRecord next()
  {
    CallDetailRecord record = new CallDetailRecord();
    if (generateCustomerInfo) {
      record.setIsdn(msidnGenerator.next());
      record.setImsi(imsiGenerator.next());
      record.setImei(imeiGenerator.next());
    }
    record.setPlan(planGenerator.next());
    record.setCallType(callTypeGenerator.next());
    record.setCorrespType(correspTypeGenerator.next());
    record.setCorrespIsdn(correspIsdnGenerator.next());

    //duration;bytes;dr
    if (CallType.MOC.label().equals(record.getCallType()) || CallType.MTC.label().equals(record.getCallType())) {
      //dr
      record.setDr(DisconnectReason.randomDisconnectReason());
      //duration
      if (DisconnectReason.NoResponse.getCode() == record.getDr()) {
        record.setDuration(random.nextInt(2) + 1);
      } else if (DisconnectReason.CallComplete.getCode() == record.getDr()) {
        record.setDuration(random.nextInt(298) + 2);
      } else if (DisconnectReason.CallDropped.getCode() == record.getDr()) {
        record.setDuration(random.nextInt(3));
      }

      //bytes: empty
    } else {
      //sms and data
      record.setDuration(0);
      //dr: empty
      if (CallType.DATA.name().equals(record.getCallType())) {
        // data
        record.setBytes(random.nextInt(1073741824) + 1);
      }
    }
    //[35, 41]
    Point point = LocationRepo.instance().getRandomPoint();
    record.setLat(point.getLat());
    record.setLon(point.getLon());

    //use current time
    record.setTime(Calendar.getInstance().getTimeInMillis());

    return record;
  }

  public boolean isGenerateCustomerInfo()
  {
    return generateCustomerInfo;
  }

  public void setGenerateCustomerInfo(boolean generateCustomerInfo)
  {
    this.generateCustomerInfo = generateCustomerInfo;
  }

}
