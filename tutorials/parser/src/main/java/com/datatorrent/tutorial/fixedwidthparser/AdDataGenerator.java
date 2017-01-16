package com.datatorrent.tutorial.fixedwidthparser;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class AdDataGenerator implements InputOperator
{
  public DefaultOutputPort<byte[]> out = new DefaultOutputPort<>();

  private transient int adId = 0;
  private transient SimpleDateFormat startDateFormat;
  private transient SimpleDateFormat endDateFormat;
  private transient Calendar calendar = Calendar.getInstance();
  private long totalTuplesEmitted = 0;
  private long tuplesCount = 10;

  private int campaignId;
  private String adName;
  private double bidPrice;
  private String startDate;
  private String endDate;
  private long securityCode;
  private boolean active;
  private boolean optimized;
  private String parentCampaign;
  private Character weatherTargeted;
  private String valid;


  @Override
  public void setup(OperatorContext context)
  {
    startDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    endDateFormat = new SimpleDateFormat("dd/MM/yyyy");
    campaignId = adId;
    adName = "TestAdd";
    bidPrice = 2.2;
    startDate = startDateFormat.format(calendar.getTime());
    endDate = endDateFormat.format(calendar.getTime());
    securityCode = adId +10;
    active=true;
    optimized=true;
    parentCampaign = "CAMP_ad123";
    weatherTargeted = 'Y';
    valid = "yes       ";

  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void emitTuples()
  {
    if (totalTuplesEmitted < tuplesCount) {
      out.emit(getTuple());
      totalTuplesEmitted++;
    }
  }

  private byte[] getTuple()
  {
    StringBuilder sb = new StringBuilder();
    sb.append(adId + "00");             //adId padding used in schema is '0'
    sb.append(campaignId  + "  ");      //campaignId padding is ' '
    sb.append("___" + adName);          //adName alignment is right and
                                        // global padding is '_'
    sb.append(bidPrice);                //bidPrice
    sb.append(startDate);               //startDate
    sb.append(endDate);                 //endDate
    sb.append(securityCode + "___");    //securityCode
    sb.append("y");                     //active
    sb.append("_");                     //optimized
    sb.append(parentCampaign);          //parentCampaign
    sb.append(weatherTargeted);         //weatherTargeted
    sb.append(valid);                   //valid
    adId++;
    return String.valueOf(sb).getBytes();
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void teardown()
  {

  }
}
