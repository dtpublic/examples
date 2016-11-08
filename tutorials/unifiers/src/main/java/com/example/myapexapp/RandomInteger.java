package com.example.myapexapp;

import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Simple operator that emits random numbers in a fixed range.
 */
public class RandomInteger extends BaseOperator implements InputOperator
{
  private transient Random rand = new Random();

  private int numTuples = 1000;
  private transient int count = 0;
  private int rangeMax = 1 << 11;

  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

  @Override
  public void beginWindow(long windowId)
  {
    count = 0;
  }

  @Override
  public void emitTuples()
  {
    if (count < numTuples) {
      ++count;
      out.emit(rand.nextInt() % rangeMax);
    }
  }

  public int getNumTuples()
  {
    return numTuples;
  }

  /**
   * Sets the number of tuples to be emitted every window.
   * @param numTuples number of tuples
   */
  public void setNumTuples(int numTuples)
  {
    this.numTuples = numTuples;
  }
}
