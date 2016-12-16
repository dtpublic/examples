package com.datatorrent.tutorial.xmlparser;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class XmlDocumentFormatter extends BaseOperator
{
  public int TupleCount;
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
  public final transient DefaultInputPort<Document> input = new DefaultInputPort<Document>()
  {
    @Override
    public void process(Document doc)
    {
      try {
      if (doc != null) {
          doc.getDocumentElement().normalize();
          Node nNode = doc.getElementsByTagName("EmployeeBean").item(0);
          Element eElement = (Element)nNode;
          String eString = "name = " + eElement.getElementsByTagName("name").item(0).getTextContent() +
                           " dept = " + eElement.getElementsByTagName("dept").item(0).getTextContent() +
                           " eid = " + eElement.getElementsByTagName("eid").item(0).getTextContent();
          output.emit(eString);
          TupleCount++;
        } else {
          System.out.println("  doc is null");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    TupleCount = 0;
  }
}


