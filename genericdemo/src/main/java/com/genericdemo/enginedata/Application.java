/**
 * Put your copyright and license info here.
 */
package com.genericdemo.enginedata;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.enrichment.FSLoader;
import com.datatorrent.contrib.enrichment.MapEnrichmentOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.contrib.enrichment.POJOEnrichmentOperator;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;
import com.datatorrent.modules.aggregation.AggregationModule;
import com.datatorrent.netlet.util.DTThrowable;
import com.google.common.collect.Maps;
import com.sun.xml.bind.v2.util.ClassLoaderRetriever;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;

@ApplicationAnnotation(name="EngineDataDemo")
public class Application implements StreamingApplication
{
  public static final String EVENT_SCHEMA = "engineDataEventSchema.json";
  public static final String DATA_SCHEMA = "engineDataDataSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String eventSchema = SchemaUtils.jarResourceFileToString(EVENT_SCHEMA);
    String dataSchema = SchemaUtils.jarResourceFileToString(DATA_SCHEMA);
    String pojoSchema = SchemaUtils.jarResourceFileToString("schema.json");
    String pojoClass = "com.genericdemo.enginedata.EngineDataEvent";

    try {
      EngineDataGenerator generator = dag.addOperator("Generator", new EngineDataGenerator());

      CsvParser parser = dag.addOperator("Parser", new CsvParser());
      parser.setClazz(EngineDataEvent.class);
      parser.setSchema(pojoSchema);

      dag.setOutputPortAttribute(parser.out, PortContext.TUPLE_CLASS, EngineDataEvent.class);
      POJOEnrichmentOperator enricher = dag.addOperator("Enricher", new POJOEnrichmentOperator());

      InputStream origIs = this.getClass().getResourceAsStream("/ErrorCodes.txt");
      File errorFile = new File("/tmp/blah1.txt");
      FileUtils.deleteQuietly(new File(errorFile.toString()));
      FileUtils.copyInputStreamToFile(origIs, errorFile);

      FSLoader store = new FSLoader();
      store.setFileName("/user/ashwin/ErrorCodes.txt");
      enricher.setLookupKeyStr("errorCode");
      enricher.setStore(store);
      enricher.setInputClassStr(pojoClass);
      enricher.setOutputClassStr(pojoClass);

      dag.setOutputPortAttribute(enricher.outputPojo, PortContext.TUPLE_CLASS, EngineDataEvent.class);
      dag.setInputPortAttribute(enricher.inputPojo, PortContext.TUPLE_CLASS, EngineDataEvent.class);

      AggregationModule aggregator = dag.addModule("Aggregations", new AggregationModule());

      //aggregator.setComputationalSchema(eventSchema);

      InputStream schemaIs = this.getClass().getResourceAsStream("/engineDataEventSchema.json");
      File schemaFile = new File("/tmp/blah.txt");
      FileUtils.deleteQuietly(new File(schemaFile.toString()));
      FileUtils.copyInputStreamToFile(schemaIs, schemaFile);

      aggregator.setComputationSchemaFilePath(schemaFile.getAbsolutePath());
      aggregator.setPojoSchema(pojoClass);
      aggregator.setTimeFieldName("time");
      aggregator.setStorePartitionCount(4);

      dag.addStream("rawdata", generator.output, parser.in);
      dag.addStream("parsed", parser.out, enricher.inputPojo);
      dag.addStream("aggregate", enricher.outputPojo, aggregator.inputPOJO);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }


}
