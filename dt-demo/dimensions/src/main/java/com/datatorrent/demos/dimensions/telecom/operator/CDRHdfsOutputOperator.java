/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.dimensions.telecom.operator;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;

public class CDRHdfsOutputOperator extends AbstractSingleFileOutputOperator<byte[]> //AbstractFileOutputOperator<byte[]>
{
  public CDRHdfsOutputOperator()
  {
    setMaxLength(64*1024*1024);
    setOutputFileName("cdr");
  }

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
  }

  @Override
  public byte[] getBytesForTuple(byte[] t)
  {
    return t;
  }
}