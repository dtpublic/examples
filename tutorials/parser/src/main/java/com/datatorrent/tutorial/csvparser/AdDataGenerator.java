package com.datatorrent.tutorial.csvparser;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;

import org.apache.commons.lang.math.RandomUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class AdDataGenerator implements InputOperator
{
  public DefaultOutputPort<byte[]> out = new DefaultOutputPort<>();

  private static final String PARENT_CAMPAIGN = "CAMP_add";
  private transient int adId = 1;
  private transient SimpleDateFormat startDateFormat;
  private transient SimpleDateFormat endDateFormat;
  private transient Calendar calendar = Calendar.getInstance();
  private long totalTuplesEmitted = 0;
  private long tuplesCount = 10;

  @Override
  public void setup(OperatorContext context)
  {
    startDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH);
    endDateFormat = new SimpleDateFormat("dd/MM/yyyy");
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
    int securityCode = adId % 30;
    if (securityCode < 10) {
      securityCode += 10;
    }

    StringBuilder sb = new StringBuilder();
    sb.append(adId + ",");                                               //adId
    sb.append((adId + 10) + ",");                                        //campaignId
    sb.append("TestAdd" + ",");                                          //adName
    sb.append("2.2" + ",");                                              //bidPrice
    sb.append(startDateFormat.format(calendar.getTime()) + ",");         //startDate
    sb.append(endDateFormat.format(calendar.getTime()) + ",");           //endDate
    sb.append(adId + 10 + ",");                                          //securityCode
    sb.append(RandomUtils.nextBoolean() + ",");                          //active
    sb.append(RandomUtils.nextBoolean() ? "OPTIMIZE," : "NO_OPTIMIZE,"); //optimized
    sb.append(PARENT_CAMPAIGN);                                          //parentCampaign
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
