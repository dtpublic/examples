package com.datatorrent.apps;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.BytesFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "Random-to-HDFS")
public class Application implements StreamingApplication
{
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator generator = dag.addOperator("POJOGenerator", POJOGenerator.class);
    BytesFileOutputOperator fileOutput = dag.addOperator("fileOutput", BytesFileOutputOperator.class);

    dag.addStream("data", generator.out, fileOutput.input);
  }
}
