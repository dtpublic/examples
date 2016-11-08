/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Operator;

import com.datatorrent.lib.util.HighLow;

import com.example.myapexapp.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  private boolean check(final boolean useUnifier, final ToConsole console)
  {
    Map<Long, List<HighLow<Integer>>> map = console.tuples;
    if (null == map || map.isEmpty()) {
      return false;
    }

    if (useUnifier) {                              // all lists should be singleton

      for (Map.Entry<Long, List<HighLow<Integer>>> entry : map.entrySet()) {
        List<HighLow<Integer>> list = entry.getValue();

        if (list.size() > 1) {
          return false;
        }
      }
      return true;

    } else {                                      // at least one list is non-singleton

      for (Map.Entry<Long, List<HighLow<Integer>>> entry : map.entrySet()) {
        List<HighLow<Integer>> list = entry.getValue();

        if (list.size() > 1) {
          return true;
        }
      }
      return false;

    }
  }

  // helper routine
  private void go(final boolean useUnifier) throws Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      conf.setBoolean("dt.application.MyFirstApplication.operator.console.prop.saveTuples",
                      true);
      if (useUnifier) {
        conf.setBoolean("dt.application.MyFirstApplication.operator.range.prop.useUnifier",
                        true);
      }
      lma.prepareDAG(new Application(), conf);
      ToConsole console = (ToConsole) lma.getDAG().getOperatorMeta("console").getOperator();
      LocalMode.Controller lc = lma.getController();
      lc.runAsync(); // runs for 10 seconds and quits

      // wait for tuples to show up
      while ( ! check(useUnifier, console) ) {
        System.out.println("Sleeping ....");
        Thread.sleep(500);
      }

    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

  @Test
  public void testApplicationWithoutUnifier() throws IOException, Exception {
    go(false);
  }

  @Test
  public void testApplicationWithUnifier() throws IOException, Exception {
    go(true);
  }

}
