package com.datatorrent.tutorial.fixedwidthparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.FixedWidthParser;
import com.datatorrent.tutorial.csvparser.FileOutputOperator;

@ApplicationAnnotation(name = "fixedWidthParserApplication")
public class fixedWidthParserApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    com.datatorrent.tutorial.fixedwidthparser.AdDataGenerator dataGenerator = dag.addOperator("dataGenerator", new com.datatorrent.tutorial.fixedwidthparser.AdDataGenerator());
    FixedWidthParser parserOperator = dag.addOperator("fixedWidthParser", new FixedWidthParser());
    FileOutputOperator dataOutput = dag.addOperator("dataOutput", new FileOutputOperator());
    FileOutputOperator errorOutput = dag.addOperator("errorOutput", new FileOutputOperator());
    FileOutputOperator pojoOutput = dag.addOperator("pojoOutput", new FileOutputOperator());

    dag.addStream("inputData", dataGenerator.out, parserOperator.in);
    dag.addStream("parsedData", parserOperator.parsedOutput, dataOutput.input);
    dag.addStream("errorData", parserOperator.err, errorOutput.input);
    dag.addStream("pojoData", parserOperator.out, pojoOutput.input);

  }
}
