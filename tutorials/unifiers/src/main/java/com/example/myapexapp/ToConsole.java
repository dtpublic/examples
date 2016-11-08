package com.example.myapexapp;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
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
  // for checking tuple counts in unit tests
  static Map<Long, List<HighLow<Integer>>> tuples;
  private boolean saveTuples;

  private long curWindowId;

  public final transient DefaultInputPort<HighLow<Integer>> in
      = new DefaultInputPort<HighLow<Integer>>() {

    @Override
    public void process(HighLow<Integer> tuple)
    {
      // tuple.toString() throws NPE if either value is null
      String t = null == tuple.getLow() ? "null" : tuple.toString();

      // timestamp in seconds
      long ts = (1000 + System.currentTimeMillis()) / 1000;
      String msg = String.format("tuple = %s, window = %d, time = %d (s)%n",
                                 t, curWindowId, ts);
      System.out.println(msg);

      // for unit tests
      if (saveTuples) {
        List<HighLow<Integer>> list = tuples.get(curWindowId);
        if (null == list) {
          list = new ArrayList<>();
          tuples.put(curWindowId, list);
        }
        list.add(tuple);
      }
    }
  };

  @Override
  public void setup(OperatorContext context)
  {
    if (saveTuples) {
      tuples = new ConcurrentHashMap<>();
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    curWindowId = windowId;
  }

  // getters and setters
  public boolean getSaveTuples() { return saveTuples; }
  public void setSaveTuples(boolean v) { saveTuples = v; }
}
