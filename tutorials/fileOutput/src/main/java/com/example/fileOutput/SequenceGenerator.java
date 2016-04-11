package com.example.fileOutput;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.Min;

/**
 * Simple operator that emits pairs of integers where the first value is the
 * operator id and the second forms elements of an arithmetic progression whose
 * increment is 'divisor' (can be changed dynamically).
 */
public class SequenceGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(SequenceGenerator.class);

  // properties

  @Min(1)
  private int maxTuples = 5;           // max number of tuples per window
  @Min(1)
  private long divisor = 1;            // only values divisible by divisor are output

  private int sleepTime;

  private long nextValue;              // next value to emit

  // transient fields

  private transient int numTuples = 0;    // number emitted in current window
  private transient long id;              // operator id
  private transient long curWindowId;     // current window id

  public final transient DefaultOutputPort<Long[]> out = new DefaultOutputPort<>();

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    long startWindowId = context.getValue(OperatorContext.ACTIVATION_WINDOW_ID);
    id = context.getId();
    sleepTime = context.getValue(OperatorContext.SPIN_MILLIS);
    LOG.debug("Leaving setup, id = {}, startWindowId = {}, sleepTime = {}, divisor = {}",
              id, startWindowId, sleepTime, divisor);
  }

  @Override
  public void beginWindow(long windowId)
  {
    numTuples = 0;
    curWindowId = windowId;
    LOG.debug("windowId = {}, divisor = {}, numTuples = {}, maxTuples = {}",
            curWindowId, divisor, numTuples, maxTuples);
    super.beginWindow(windowId);
  }

  @Override
  public void endWindow() {
    LOG.debug("windowId = {}, divisor = {}, numTuples = {}, maxTuples = {}",
            curWindowId, divisor, numTuples, maxTuples);
    super.endWindow();
  }

  @Override
  public void emitTuples()
  {
    LOG.debug("numTuples = {}, curWindowId = {}", numTuples, curWindowId);
    if (numTuples < maxTuples) {
      // nextValue will normally be divisible by divisor but the divisor can be changed
      // externally (e.g. after a repartition) so find next value that is divisible by divisor
      //
      final long rem = nextValue % divisor;
      if (0 != rem) nextValue += (divisor - rem);
      ++numTuples;
      out.emit(new Long[]{id, nextValue});
      nextValue += divisor;
    } else {

      try {
        // avoid repeated calls to this function
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.info("Sleep interrupted");
      }
    }
  }

  // getters and setters

  public long getDivisor() { return divisor; }
  public void setDivisor(long v) { divisor = v; }
  public int getMaxTuples() { return maxTuples; }
  public void setMaxTuples(int v) { maxTuples = v; }
}
