/**
 * Put your copyright and license info here.
 */
package com.example.fileOutput;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    SequenceGenerator generator = dag.addOperator("generator", SequenceGenerator.class);

    FileWriter writer = dag.addOperator("writer", FileWriter.class);
    writer.setMaxLength(1 << 10);
    writer.setFilePath("/tmp/fileOutput");
    writer.setFileName("sequence");

    //Add if needed: .setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("data", generator.out, writer.input);
  }
}
