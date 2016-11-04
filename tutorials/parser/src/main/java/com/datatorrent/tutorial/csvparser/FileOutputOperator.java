package com.datatorrent.tutorial.csvparser;

import javax.validation.constraints.NotNull;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class FileOutputOperator extends AbstractFileOutputOperator<Object>
{
  @NotNull
  private String outputFileName;

  @Override
  protected String getFileName(Object tuple)
  {
    return outputFileName;
  }

  @Override
  protected byte[] getBytesForTuple(Object tuple)
  {
    return (tuple.toString() + "\n").getBytes();
  }

  public String getOutputFileName()
  {
    return outputFileName;
  }

  public void setOutputFileName(String outputFileName)
  {
    this.outputFileName = outputFileName;
  }
}
