package com.datatorrent.tutorials.operatorTutorial;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

public class WordCountOperatorTest
{

  private final static String STOP_WORD_FILE_PATH= "src/test/resources/stop-words";
  private static WordCountOperator wordCountOperator = new WordCountOperator();
  private static CollectorTestSink<Long> sink;

  @Before
  public void setup()
  {
    Path wd = null;
    try {
      FileSystem fs = FileSystem.get(new Configuration());
      wd = fs.getWorkingDirectory();
    } catch (IOException e) {
      e.printStackTrace();
    }

    // Create a word count operator and call the setup method. setup() will actually be called by the Apache Apex engine during runtime.
    wordCountOperator = new WordCountOperator();
    String path = wd.toString()+"/"+STOP_WORD_FILE_PATH;
    wordCountOperator.setStopWordFilePath(path);
    wordCountOperator.setup(null);

    // Create a dummy sink to simulate the output port and set it as the output port of the operator
    sink = new CollectorTestSink<Long>();
    TestUtils.setSink(wordCountOperator.output, sink);
  }

  @Test
  public void testWordCountPerTuple()
  {
    // Set the per tuple option to true
    wordCountOperator.setSendPerTuple(true);

    wordCountOperator.beginWindow(0);
    wordCountOperator.input.process("Humpty dumpty sat on a wall");
    wordCountOperator.input.process("Humpty dumpty had a great fall");
    wordCountOperator.endWindow();

    Assert.assertEquals(8, sink.collectedTuples.size());
  }

  @Test
  public void testWordCountPerWindow()
  {
    // Set the per tuple option to true
    wordCountOperator.setSendPerTuple(false);

    wordCountOperator.beginWindow(0);
    wordCountOperator.input.process("Humpty dumpty sat on a wall");
    wordCountOperator.input.process("Humpty dumpty had a great fall");
    wordCountOperator.endWindow();

    Assert.assertEquals(6, sink.collectedTuples.size());
  }

  @After
  public void teardown()
  {
    // Teardown the operator gracefully
    wordCountOperator.teardown();
  }
}
