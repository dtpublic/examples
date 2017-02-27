package com.datatorrent.tutorial.regexparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.RegexParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.tutorial.csvparser.FileOutputOperator;

@ApplicationAnnotation(name = "RegexParser")
public class RegexParserApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ServerLogGenerator logGenerator = dag.addOperator("logGenerator", ServerLogGenerator.class);
    RegexParser regexParser = dag.addOperator("regexParser", RegexParser.class);
    dag.setOutputPortAttribute(regexParser.out, Context.PortContext.TUPLE_CLASS, ServerLog.class);
    FileOutputOperator regexWriter = dag.addOperator("regexWriter", FileOutputOperator.class);
    FileOutputOperator regexErrorWriter = dag.addOperator("regexErrorWriter", FileOutputOperator.class);

    dag.addStream("regexInput", logGenerator.outputPort, regexParser.in);
    dag.addStream("regexOutput", regexParser.out, regexWriter.input);
    dag.addStream("regexError", regexParser.err, regexErrorWriter.input);
  }
}
