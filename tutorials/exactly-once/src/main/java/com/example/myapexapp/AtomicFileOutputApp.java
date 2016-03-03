/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.example.myapexapp;

import org.joda.time.Duration;

import org.apache.hadoop.conf.Configuration;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;
import com.datatorrent.lib.util.KeyValPair;

@ApplicationAnnotation(name = "AtomicFileOutput")
public class AtomicFileOutputApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    kafkaInput.setIdempotentStorageManager(new IdempotentStorageManager.FSIdempotentStorageManager());

    Application.UniqueCounterFlat count = dag.addOperator("count", new Application.UniqueCounterFlat());

    FileWriter fileWriter = dag.addOperator("fileWriter", new FileWriter());

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("words", kafkaInput.outputPort, count.data);
    dag.addStream("counts", count.counts, fileWriter.input, cons.input);
  }

  public static class FileWriter extends AbstractFileOutputOperator<KeyValPair<String, Integer>>
      implements Operator.IdleTimeHandler
  {
    static final String FILE_NAME = "filestore";

    @FieldSerializer.Bind(value = JavaSerializer.class)
    private Duration idleTimeDuration = Duration.standardSeconds(10);

    private long lastProcessedTime;

    @Override
    public void setup(Context.OperatorContext context)
    {
      lastProcessedTime = System.currentTimeMillis();
      super.setup(context);
    }

    @Override
    protected String getFileName(KeyValPair<String, Integer> keyValPair)
    {
      return FILE_NAME;
    }

    @Override
    protected byte[] getBytesForTuple(KeyValPair<String, Integer> keyValPair)
    {
      return (keyValPair.toString()+"\n").getBytes();
    }

    @Override
    protected void processTuple(KeyValPair<String, Integer> tuple)
    {
      super.processTuple(tuple);
      lastProcessedTime = System.currentTimeMillis();
    }

    @Override
    public void handleIdleTime()
    {
      //request for finalization once there is no input. This is done automatically if the file is rotated periodically or has a size threshold.
      if (System.currentTimeMillis() - lastProcessedTime > idleTimeDuration.getMillis()) {
        requestFinalize(FILE_NAME);
        lastProcessedTime = System.currentTimeMillis();
      } else {
        try {
          Thread.sleep(500L);
        } catch (InterruptedException e) {
          throw new RuntimeException("interrupted");
        }
      }
    }

    public Duration getIdleTimeDuration()
    {
      return idleTimeDuration;
    }

    public void setIdleTimeDuration(Duration idleTimeDuration)
    {
      this.idleTimeDuration = idleTimeDuration;
    }
  }
}
