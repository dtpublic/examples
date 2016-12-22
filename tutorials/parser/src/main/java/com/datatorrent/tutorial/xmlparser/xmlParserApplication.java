package com.datatorrent.tutorial.xmlparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.parser.XmlParser;
import com.datatorrent.tutorial.csvparser.FileOutputOperator;

@ApplicationAnnotation(name = "xmlParserApplication")
public class xmlParserApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EmployeeDataGenerator dataGenerator = dag.addOperator("dataGenerator", new EmployeeDataGenerator());
    XmlParser parserOperator = dag.addOperator("xmlParser", new XmlParser());
    XmlDocumentFormatter resultCollector = dag.addOperator("resultCollector", new XmlDocumentFormatter());
    FileOutputOperator pojoOutput = dag.addOperator("pojoOutput", new FileOutputOperator());
    FileOutputOperator dataOutput = dag.addOperator("dataOutput", new FileOutputOperator());
    FileOutputOperator errorOutput = dag.addOperator("errorOutput", new FileOutputOperator());

    JavaSerializationStreamCodec codec = new JavaSerializationStreamCodec();
    dag.setInputPortAttribute(resultCollector.input, Context.PortContext.STREAM_CODEC, codec);

    dag.addStream("inputData", dataGenerator.output, parserOperator.in);
    dag.addStream("parsedDoc", parserOperator.parsedOutput, resultCollector.input);
    dag.addStream("formattedData", resultCollector.output, dataOutput.input);
    dag.addStream("errorData", parserOperator.err, errorOutput.input);
    dag.addStream("pojotoFile", parserOperator.out,pojoOutput.input);
  }
}
