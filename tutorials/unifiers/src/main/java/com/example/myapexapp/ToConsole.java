package com.example.myapexapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.UnifierRange;

/**
 * Simple operator that emits received tuples to the console along with a timestamp
 */
public class ToConsole extends BaseOperator
{
  private long curWindowId;

  public final transient DefaultInputPort<HighLow<Integer>> in
      = new DefaultInputPort<HighLow<Integer>>() {

    @Override
    public void process(HighLow<Integer> tuple)
    {
      // tuple.toString() throws NPE if either value is null
      String t = null == tuple.getLow() ? "null" : tuple.toString();
      String msg = String.format("tuple = %s, window = %d, time = %d (s)%n",
                                 t, curWindowId, System.currentTimeMillis());
      System.out.println(msg);
    }
  };

  @Override
  public void beginWindow(long windowId)
  {
    curWindowId = windowId;
  }

  @Override
  public void endWindow()
  {
  }

}
