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
import com.datatorrent.contrib.hive.FSPojoToHiveOperator;
import com.datatorrent.contrib.hive.FSPojoToHiveOperator.FIELD_TYPE;
import com.datatorrent.contrib.hive.HiveOperator;
import com.datatorrent.contrib.hive.HiveStore;
import com.datatorrent.contrib.parser.CsvParser;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.ConsoleOutputOperator;
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
import java.util.ArrayList;
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
      //parser.setClazz(EngineDataEvent.class);
      parser.setSchema(pojoSchema);

      dag.setOutputPortAttribute(parser.out, PortContext.TUPLE_CLASS, EngineDataEvent.class);
      POJOEnrichmentOperator enricher = dag.addOperator("Enricher", new POJOEnrichmentOperator());

      //InputStream origIs = this.getClass().getResourceAsStream("/ErrorCodes.txt");
      //File errorFile = new File("/tmp/blah1.txt");
      //FileUtils.deleteQuietly(new File(errorFile.toString()));
      //FileUtils.copyInputStreamToFile(origIs, errorFile);

      FSLoader store = new FSLoader();
      store.setFileName("/user/ashwin/ErrorCodes.txt");

      enricher.setLookupKeyStr("errorCode");
      enricher.setStore(store);
      //enricher.setInputClassStr(pojoClass);
      //enricher.setOutputClassStr(pojoClass);
      enricher.setTupleFieldsToCopyFromInputToOutputStr("time,errorCode,model,gear,temperature,speed,rpm");
      enricher.setFieldToAddToOutputTupleStr("description");

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
      //aggregator.setStorePartitionCount(4);
      //aggregator.setDimensionComputationPartitionCount(4);

      //ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

      HiveStore hiveStore = new HiveStore();
      hiveStore.setDatabaseUrl("jdbc:hive2://localhost:10000");
      hiveStore.setDatabaseDriver("org.apache.hive.jdbc.HiveDriver");
      hiveStore.setUserName("ashwin");
      hiveStore.setFilepath("/user/ashwin/hive/");

      ArrayList<String> hivePartitionColumns = new ArrayList<String>();
      //hivePartitionColumns.add("model");
      ArrayList<FIELD_TYPE> partitiontypes = new ArrayList<FIELD_TYPE>();
      partitiontypes.add(FIELD_TYPE.STRING);

      ArrayList<FIELD_TYPE> fieldtypes = new ArrayList<FIELD_TYPE>();
      fieldtypes.add(FIELD_TYPE.LONG);
      fieldtypes.add(FIELD_TYPE.STRING);
      fieldtypes.add(FIELD_TYPE.STRING);
      fieldtypes.add(FIELD_TYPE.INTEGER);
      fieldtypes.add(FIELD_TYPE.DOUBLE);
      fieldtypes.add(FIELD_TYPE.DOUBLE);
      fieldtypes.add(FIELD_TYPE.DOUBLE);
      fieldtypes.add(FIELD_TYPE.STRING);

      ArrayList<String> hiveColumns = new ArrayList<String>();
      hiveColumns.add("time");
      hiveColumns.add("errorCode");
      hiveColumns.add("model");
      hiveColumns.add("gear");
      hiveColumns.add("temperature");
      hiveColumns.add("speed");
      hiveColumns.add("rpm");
      hiveColumns.add("description");


      FSPojoToHiveOperator fsRolling = dag.addOperator("HdfsFileWriter", new FSPojoToHiveOperator());
      fsRolling.setFilePath("/user/ashwin/hive/");
      fsRolling.setHiveColumns(hiveColumns);
      fsRolling.setHiveColumnDataTypes(fieldtypes);
      fsRolling.setHivePartitionColumnDataTypes(partitiontypes);
      fsRolling.setHivePartitionColumns(hivePartitionColumns);

      ArrayList<String> expressions = new ArrayList<String>();
      expressions.add("getTime()");
      expressions.add("getErrorCode()");
      expressions.add("getModel()");
      expressions.add("getGear()");
      expressions.add("getTemperature()");
      expressions.add("getSpeed()");
      expressions.add("getRpm()");
      expressions.add("getDescription()");

      ArrayList<String> expressionsPartitions = new ArrayList<String>();

      //expressionsPartitions.add("getModel()");
      fsRolling.setMaxLength(1000000);
      //fsRolling.setRotationWindows(60);
      fsRolling.setAlwaysWriteToTmp(false);
      fsRolling.setExpressionsForHiveColumns(expressions);
      fsRolling.setExpressionsForHivePartitionColumns(expressionsPartitions);

      HiveOperator hiveOperator = dag.addOperator("HiveOperator", new HiveOperator());
      hiveOperator.setHivestore(hiveStore);
      hiveOperator.setTablename("errorcodes");
      hiveOperator.setHivePartitionColumns(hivePartitionColumns);

      dag.addStream("rawdata", generator.output, parser.in);
      dag.addStream("parsed", parser.out, enricher.inputPojo);
      dag.addStream("aggregate", enricher.outputPojo, aggregator.inputPOJO, fsRolling.input);
      dag.addStream("toHive", fsRolling.outputPort, hiveOperator.input);
    }
    catch (Exception ex) {
      DTThrowable.rethrow(ex);
    }
  }


}
