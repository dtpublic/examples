package com.datatorrent.tutorial.csvparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name = "csvParseApplication")
public class csvParserApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    AdDataGenerator dataGenerator = dag.addOperator("dataGenerator", new AdDataGenerator());
    CsvParser parserOperator = dag.addOperator("csvParser", new CsvParser());
    FileOutputOperator dataOutput = dag.addOperator("dataOutput", new FileOutputOperator());
    FileOutputOperator errorOutput = dag.addOperator("errorOutput", new FileOutputOperator());
    ConsoleOutputOperator consoleOutput = dag.addOperator("consoleOutput", new ConsoleOutputOperator());

    dag.addStream("inputData", dataGenerator.out, parserOperator.in);
    dag.addStream("parsedData", parserOperator.parsedOutput, dataOutput.input);
    dag.addStream("errorData", parserOperator.err, errorOutput.input);
    dag.addStream("pojoData", parserOperator.out, consoleOutput.input);

  }
}
