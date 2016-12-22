package com.datatorrent.tutorial.xmlparser;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class EmployeeDataGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();
  public int tupleCount;
  public int totalTupleCount = 10;

  @Override
  public void emitTuples()
  {
    if (tupleCount < totalTupleCount) {
      StringBuilder xmlSample = new StringBuilder();
      xmlSample.append("<?xml version=\"1.0\"?>");
      xmlSample.append("<EmployeeBean>");
      xmlSample.append("<name>employee" + tupleCount + "</name>");
      xmlSample.append("<dept>department"+ tupleCount + "</dept>");
      xmlSample.append("<eid>" + tupleCount +"</eid>");
      xmlSample.append("<dateOfJoining>2015-01-01</dateOfJoining>");
      xmlSample.append("<address>"+ "<city>new york</city>");
      xmlSample.append("<country>US</country>" +"</address>");
      xmlSample.append("</EmployeeBean>");
      output.emit(xmlSample.toString());
      tupleCount++;
    }
  }
  @Override
  public void setup(Context.OperatorContext context)
  {
    tupleCount = 0;
  }
}
