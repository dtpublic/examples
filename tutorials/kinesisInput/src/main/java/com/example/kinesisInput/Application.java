/**
 * Put your copyright and license info here.
 */
package com.example.kinesisInput;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kinesis.KinesisStringInputOperator;

@ApplicationAnnotation(name="Kinesis-to-HDFS")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    KinesisStringInputOperator inputOperator = dag.addOperator("kinesisInput", new KinesisStringInputOperator());
    GenericFileOutputOperator.StringFileOutputOperator fileOutputOperator = dag.addOperator("fileOutput", new GenericFileOutputOperator.StringFileOutputOperator());
    dag.addStream("kinesis-to-hdfs", inputOperator.outputPort, fileOutputOperator.input);
  }
}
