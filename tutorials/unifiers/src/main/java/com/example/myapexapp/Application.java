package com.example.myapexapp;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // create operators
    RandomInteger random = dag.addOperator("random", new RandomInteger());
    RangeFinder rf = dag.addOperator("range", new RangeFinder());
    ToConsole cons = dag.addOperator("console", new ToConsole());

    // create streams
    dag.addStream("randomData", random.out, rf.in);
    dag.addStream("rangeData", rf.out, cons.in);
  }
}
