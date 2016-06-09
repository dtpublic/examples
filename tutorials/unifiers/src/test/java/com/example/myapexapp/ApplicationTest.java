/**
 * Put your copyright and license info here.
 */
package com.example.myapexapp;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.example.myapexapp.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {

  // helper routine
  private void go(final boolean useUnifier) throws Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      if (useUnifier) {
        conf.setBoolean("dt.application.MyFirstApplication.operator.range.prop.useUnifier",
                        true);
      }
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000); // runs for 10 seconds and quits
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
