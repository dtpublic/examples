package com.datatorrent.cassandra;

import java.util.List;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.cassandra.CassandraPOJOInputOperator;
import com.datatorrent.contrib.cassandra.CassandraTransactionalStore;
import com.datatorrent.lib.converter.Converter;
import com.datatorrent.lib.util.FieldInfo;
import com.google.common.collect.Lists;

/**
 * Application to test Cassandra Input Operator from Apache Apex-Malhar library
 * Application has following operators:<br/>
 *
 * @author priyanka
 *
 */
@ApplicationAnnotation(name = "CassandraInputApplication")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("id", "id", null));
    fieldInfos.add(new FieldInfo("city", "city", null));
    fieldInfos.add(new FieldInfo("fname", "firstName", null));
    fieldInfos.add(new FieldInfo("lname", "lastName", null));

    CassandraTransactionalStore transactionalStore = new CassandraTransactionalStore();
    CassandraPOJOInputOperator cassandraInput = dag.addOperator("CassandraReader", new CassandraPOJOInputOperator());
    cassandraInput.setStore(transactionalStore);
    cassandraInput.setFieldInfos(fieldInfos);
    GenericFileOutputOperator<Object> fileOutput = dag.addOperator("fileWriter", new GenericFileOutputOperator<>());
    fileOutput.setConverter(new ObjectConverter());

    dag.addStream("CassandraToFile", cassandraInput.outputPort, fileOutput.input);
  }

  public static class ObjectConverter implements Converter<Object, byte[]>
  {

    private static transient Logger logger = LoggerFactory.getLogger(ObjectConverter.class);

    @Override
    public byte[] convert(Object userInfo)
    {
      logger.info("userInfo: " + userInfo.toString());
      return userInfo.toString().getBytes();
    }

  }
}
