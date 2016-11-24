/**
 * Put your copyright and license info here.
 */
package com.datatorrent.maprapp;

import java.io.IOException;

import javax.validation.ConstraintViolationException;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import com.datatorrent.api.LocalMode;
import com.datatorrent.maprapp.Application;

/**
 * Test the DAG declaration in local mode.
 */
public class ApplicationTest {
  boolean check() {
    return true;
  }

  @Test
  public void testApplication() throws IOException, Exception {
    try {
      LocalMode lma = LocalMode.newInstance();
      Configuration conf = new Configuration(false);
      conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.runAsync();
      while( !check() ) {
        System.out.println("Sleeping...");
        Thread.sleep(1000);
      }
    } catch (ConstraintViolationException e) {
      Assert.fail("constraint violations: " + e.getConstraintViolations());
    }
  }

}
