package com.datatorrent.apps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.common.util.Pair;
import com.datatorrent.metrics.api.aggregation.NumberAggregate;
import com.datatorrent.metrics.api.appmetrics.DefaultAppMetricProcessor;

public class AppMetricsService extends DefaultAppMetricProcessor
{
  private final Logger LOG = LoggerFactory.getLogger(AppMetricsService.class);
  private static final Set<String> operators = new HashSet<String>(Arrays.asList("csvParser", "filter", "POJOGenerator"));

  @Override
  public Map<String, Object> computeAppLevelMetrics(Map<String, Map<String, Object>> completedMetrics)
  {
    Long incoming = (Long) completedMetrics.get("csvParser").get("incomingTuplesCount");
    Long filtered = (Long) completedMetrics.get("filter").get("trueTuples");
    Long windowedRecordSize = (Long)completedMetrics.get("POJOGenerator").get("windowedRecordSize");
    Long recordCount = (Long)completedMetrics.get("POJOGenerator").get("emittedRecordCount");

    Map<String, Object> output = Maps.newHashMap();
    if(incoming != null && filtered != null){
      if(incoming != 0){
        double percentFiltered = (filtered * 100.0) /incoming;
        output.put("percentFiltered", percentFiltered);
      }
    }

    if ((windowedRecordSize != null) && (recordCount != null)) {
      if (recordCount != 0) {
        double averageRecordSize = new Double(windowedRecordSize) / new Double(recordCount);
        output.put("avgRecordSize", averageRecordSize);
      }
    }

    Collection<Collection<Pair<String, Object>>> ccp = Lists.newArrayList();
    for (Map.Entry<String, Map<String, Object>> e1 : completedMetrics.entrySet()) {
      for (Map.Entry<String, Object> e2 : e1.getValue().entrySet()) {
        Object metricValue = e2.getValue();
        Collection<Pair<String, Object>> row = Lists.newArrayList();
        row.add(new Pair<String, Object>("MetricName", e1.getKey() + "." + e2.getKey()));
        row.add(new Pair<>("MetricValue", metricValue));
        ccp.add(row);
      }
    }
    if (!ccp.isEmpty()) {
      output.put("AllMetrics", ccp);
    }

    return output;
  }

  public static class AppMetricAggregatorImpl implements AppMetricAggregator
  {
    @Override
    public Map<String, Object> aggregateAppLevelMetrics(Map<String, Map<String, NumberAggregate>> aggregates)
    {
      Long incoming = (Long) aggregates.get("csvParser").get("incomingTuplesCount").getSum();
      Long filtered = (Long) aggregates.get("filter").get("trueTuples").getSum();
      Long windowedRecordSize = (Long)aggregates.get("POJOGenerator").get("windowedRecordSize").getSum();
      Long recordCount = (Long)aggregates.get("POJOGenerator").get("emittedRecordCount").getSum();

      Map<String, Object> output = Maps.newHashMap();
      if(incoming != null && filtered != null){
        if(incoming != 0){
          double percentFiltered = (filtered * 100.0) /incoming;
          output.put("percentFiltered", percentFiltered);
        }
      }

      if ((windowedRecordSize != null) && (recordCount != null)) {
        if (recordCount != 0) {
          double averageRecordSize = new Double(windowedRecordSize) / new Double(recordCount);
          output.put("avgRecordSize", averageRecordSize);
        }
      }

      return output;
    }

    @Override
    public Set<String> getRequiredOperators()
    {
      return operators;
    }

  }
  private static final long serialVersionUID = 5119330693347067792L;
}
