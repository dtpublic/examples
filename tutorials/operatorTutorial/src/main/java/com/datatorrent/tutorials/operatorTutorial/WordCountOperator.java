package com.datatorrent.tutorials.operatorTutorial;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;
import com.google.common.collect.Maps;

/**
 * Basic implementation of a Word Count operator
 * Input: String
 * Output: Entry<String, Long> - <Word, Count>
 * Output can be per tuple or per window. sendPerTuple parameter controls this.
 */
public class WordCountOperator extends BaseOperator
{
  /**
   * Defines whether to send data to the output port after each tuple or each window
   */
  private boolean sendPerTuple = true; // Default
  /**
   * File path for the stop-word file in HDFS
   */
  private String stopWordFilePath;

  /**
   * Array to hold the list of stop words
   */
  private transient String[] stopWords;

  /**
   * Global counts for all the words
   * This will have counts for all the words encountered since the activation of the operator.
   */
  private Map<String, Long> globalCounts;

  /**
   * Counts for only the changed words
   * This will be per tuple or per window depending on the configuration parameter - sendPerTule
   * This is transient variable since it is okay if we lose the updated counts on an operator crash. We have the non-transient backups in globalCounts.
   */
  private transient Map<String, Long> updatedCounts;

  /**
   * Defines Input Port - DefaultInputPort
   * Accepts data from the upstream operator
   * Type String
   */
  public transient DefaultInputPort<String> input = new DefaultInputPort<String>() {
    /*
     * Its is a good idea to take the processing logic out of the process() call.
     * This allows for extending this operator into a different behavior by overriding processTuple() call.
     */
    @Override
    public void process(String tuple)
    {
      processTuple(tuple);
    }
  };

  /**
   * Defines Output Port - DefaultOutputPort
   * Sends data to the down stream operator which can consume this data
   * Type Map<String, Long>
   */
  public transient DefaultOutputPort<Entry<String, Long>> output = new DefaultOutputPort<Entry<String,Long>>();

  public WordCountOperator()
  {
    // Initialize globalCounts since this is non transient and the value of this will be retained until the operator is deactivated.
    globalCounts = Maps.newHashMap();
  }

  /**
   * Setup call
   */
  @Override
  public void setup(OperatorContext context)
  {
    /*
     * Read the stop-word file from HDFS
     * Assume the format to be a space/newline separated list of stop-words
     */
    String line = "";
    BufferedReader br = null;
    try{
      Configuration conf = new Configuration();
      FileSystem fs = FileSystem.get(conf);
      Path filePath = new Path(getStopWordFilePath());
      br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
      StringBuilder stopWordText = new StringBuilder();
      while((line = br.readLine()) != null)
      {
        stopWordText.append(line.toLowerCase()+"\n");
      }
      br.close();
      stopWords = stopWordText.toString().split("[ \n]");
    } 
    catch (IOException e) {
      throw new RuntimeException("Exception in reading stop word file", e);
    }

    // Initialize updatedCounts in setup call since setup may be called at the start of the operator lifetime or after a crash. Being a transient variable, it will lose all the data.
    updatedCounts = Maps.newHashMap();
  }

  /**
   * Begin window call for the operator.
   * If sending counts per window, clear the counts at the start of the window
   * @param windowId
   */
  public void beginWindow(long windowId)
  {
    if(!sendPerTuple) // Then counts to be kept per window
    {
      updatedCounts.clear();
    }
  }

  /**
   * Defines what should be done with each incoming tuple
   * Update gobalCounts and updatedCounts.
   * If sending output per tuple, clear the updatedCounts and then send out the updatedCounts after processing the tuple
   */
  protected void processTuple(String tuple)
  {
    if(sendPerTuple)
    {
      updatedCounts.clear();
    }
    String[] words = tuple.toLowerCase().split("[ ]");
    for(String word: words)
    {
      if( ! Arrays.asList(stopWords).contains(word))
      {
        if(globalCounts.containsKey(word)) {
          globalCounts.put(word, globalCounts.get(word)+1);
        }
        else {
          globalCounts.put(word, 1L);
        }
        updatedCounts.put(word, globalCounts.get(word));
      }
    }
    if(sendPerTuple)
    {
      for(Entry<String, Long> entry: updatedCounts.entrySet())
      {
        output.emit(entry);
      }
    }
  }

  /**
   * End window call for the operator
   * If sending per window, emit the updated counts here.
   */
  @Override
  public void endWindow()
  {
    if(!sendPerTuple)
    {
      for(Entry<String, Long> entry: updatedCounts.entrySet())
      {
        output.emit(entry);
      }
    }
  }

  /*
   * Getters and setters for Operator properties
   */
  public boolean isSendPerTuple()
  {
    return sendPerTuple;
  }

  public void setSendPerTuple(boolean sendPerTuple)
  {
    this.sendPerTuple = sendPerTuple;
  }

  public String getStopWordFilePath()
  {
    return stopWordFilePath;
  }

  public void setStopWordFilePath(String stopWordFilePath)
  {
    this.stopWordFilePath = stopWordFilePath;
  }

}
