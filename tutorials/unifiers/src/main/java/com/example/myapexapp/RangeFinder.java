/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.util.HighLow;
import com.datatorrent.lib.util.UnifierRange;

/**
 * This is a simple operator that emits the maximum and minimum of values seen in a
 * window.
 */
public class RangeFinder extends BaseOperator
{
  private boolean useUnifier;

  private transient HighLow<Integer> range = new HighLow<>(Integer.MIN_VALUE,
                                                           Integer.MAX_VALUE);

  public final transient DefaultInputPort<Integer> in
      = new DefaultInputPort<Integer>() {

    @Override
    public void process(Integer tuple)
    {
      if (null == range.getLow() || null == range.getHigh()) {    // should never happen
        range.setLow(tuple);
        range.setHigh(tuple);
      } else {
        if (tuple < range.getLow()) {
          range.setLow(tuple);
        }
        if (tuple > range.getHigh()) {
          range.setHigh(tuple);
        }
      }
    }
  };

  public final transient DefaultOutputPort<HighLow<Integer>> out
    = new DefaultOutputPort<HighLow<Integer>>() {
    public Unifier<HighLow<Integer>> getUnifier() {
      return  useUnifier ? new UnifierRange<Integer>() : super.getUnifier();
    }
  };

  @Override
  public void endWindow()
  {
    out.emit(range);
    range = new HighLow<>(Integer.MIN_VALUE, Integer.MAX_VALUE);
  }

  // getters and setters
  public boolean getUseUnifier() { return useUnifier; }
  public void setUseUnifier(boolean v) { useUnifier = v; }
}
