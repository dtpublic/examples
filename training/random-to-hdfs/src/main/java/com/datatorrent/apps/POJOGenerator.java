package com.datatorrent.apps;

import java.util.Random;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;


public class POJOGenerator extends BaseOperator implements InputOperator
{
  private transient final String[] countryNames = {"USA", "India", "China", "Japan", "UK", "Germany", "Sweden", "Australia", "Canada", "UAE", "Russia", "France", "Brazil", "Malaysia", "New zealand"};
  private transient final int[] weights = {10,7,2,2,5,9,7,8,5,10,5,6,7,7,6};

  Random r = new Random();

  private int MAX_EMITS = 1000;

  @AutoMetric
  private int emittedRecordCount;

  @AutoMetric
  private int windowedRecordSize;


  public final transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    emittedRecordCount = 0;
    windowedRecordSize = 0;
  }

  @Override
  public void emitTuples()
  {
    if (emittedRecordCount++ < MAX_EMITS) {
      byte[] randomPOJO = createRandomPOJO();
      windowedRecordSize += randomPOJO.length;
      out.emit(randomPOJO);
    }
  }

  private byte[] createRandomPOJO()
  {
    int code = r.nextInt(countryNames.length);
    String name = countryNames[code];
    int amount = (r.nextInt(300 - 10) + 10) * weights[code];

    return (code + "|" + name + "|" + amount).getBytes();
  }
}
