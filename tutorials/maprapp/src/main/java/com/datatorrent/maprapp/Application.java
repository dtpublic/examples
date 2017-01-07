/**
 * Put your copyright and license info here.
 */
package com.datatorrent.maprapp;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.contrib.hbase.HBaseFieldInfo;
import com.datatorrent.contrib.hbase.HBasePOJOPutOperator;
import com.datatorrent.contrib.hbase.HBaseStore;
import com.datatorrent.contrib.parser.JsonParser;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.TableInfo;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.DAG.Locality;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@ApplicationAnnotation(name="MaprStreamsToMaprDB")
public class Application implements StreamingApplication
{
  private void createHBaseTable(HBaseStore store)
  {
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(store.getConfiguration());
      final String tableName = store.getTableName();

      if(!admin.isTableAvailable(tableName))
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
    } catch (Exception ex) {
      LOG.warn("exception", ex);
    } finally {
      if (admin != null)
      {
        try {
          admin.close();
        } catch (Exception e) {
          LOG.warn("close admin exception", e);
        }
      }
    }
  }

  private HBasePOJOPutOperator configureHBaseOperator(HBasePOJOPutOperator operator)
  {
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<>();

    tableInfo.setRowOrIdExpression("row");

    List<HBaseFieldInfo> fieldInfoList = new ArrayList<>();
    fieldInfoList.add(new HBaseFieldInfo("id", "id", FieldInfo.SupportType.INTEGER, "f0"));
    fieldInfoList.add(new HBaseFieldInfo("name", "name", FieldInfo.SupportType.STRING, "f1"));
    fieldInfoList.add(new HBaseFieldInfo("message", "message", FieldInfo.SupportType.STRING, "f1"));

    tableInfo.setFieldsInfo(fieldInfoList);
    operator.setTableInfo(tableInfo);

    HBaseStore store = new HBaseStore();
    store.setTableName("test");
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);

    createHBaseTable(store);

    operator.setStore(store);

    return operator;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    KafkaSinglePortInputOperator maprStreams = dag.addOperator("Streams", new KafkaSinglePortInputOperator());
    JsonParser parser = dag.addOperator("Parser", JsonParser.class);
    HBasePOJOPutOperator maprDb = dag.addOperator("Db", configureHBaseOperator(new HBasePOJOPutOperator()));
    dag.addStream("Streams2Parser", maprStreams.outputPort, parser.in).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("Parser2Db", parser.out, maprDb.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  private static final Logger LOG = LoggerFactory.getLogger(StreamingApplication.class);
}
