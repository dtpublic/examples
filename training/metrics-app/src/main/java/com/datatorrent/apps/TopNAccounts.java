package com.datatorrent.apps;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.mutable.MutableLong;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.Pair;

/**
 * Created by chinmay on 29/5/17.
 */
public class TopNAccounts extends BaseOperator
{
  @FieldSerializer.Bind(JavaSerializer.class)
  @AutoMetric
  private Collection<Collection<Pair<String, Object>>> topCountrySum = new ArrayList<>();

  @FieldSerializer.Bind(JavaSerializer.class)
  @AutoMetric
  private Collection<Collection<Pair<String, Object>>> topCountryCount = new ArrayList<>();

  @AutoMetric
  private long totalSum = 0;

  @AutoMetric
  private long totalCount = 0;

  private Map<String, MutableLong> collectivesSum = new HashMap<>();
  private Map<String, MutableLong> collectivesCount = new HashMap<>();

  public final transient DefaultInputPort<Object> in = new DefaultInputPort<Object>()
  {
    @Override
    public void process(Object t)
    {
      PojoEvent tuple = (PojoEvent)t;
      String name = tuple.getName();
      int amount = tuple.getAmount();

     updateCollectives(collectivesSum, name, amount);
     updateCollectives(collectivesCount, name, 1);

     totalSum += amount;
     totalCount += 1;
    }
  };

  private void updateCollectives(Map<String, MutableLong> collectives, String name, int value){

    MutableLong v = collectives.get(name);
    if (v == null) {
      collectives.put(name, new MutableLong(value));
    } else {
      v.add(value);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);

    totalSum = 0;
    totalCount = 0;

    collectivesSum.clear();
    collectivesCount.clear();

    LOG.debug("topCountrySum {}", topCountrySum);
    LOG.debug("topCountryCount {}", topCountryCount);
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    updateSummaryResults(collectivesSum, topCountrySum);
    updateSummaryResults(collectivesCount, topCountryCount);
  }

  private void updateSummaryResults(Map<String, MutableLong> collectives,
      Collection<Collection<Pair<String, Object>>> results){

    Map<String, Long> unsortedMap = new HashMap<>();
    for (Map.Entry<String, MutableLong> entry : collectives.entrySet()) {
      unsortedMap.put(entry.getKey(), entry.getValue().getValue());
    }

    Map<String, Long> sortedMap = sortByValue(unsortedMap);
    results.clear();
    if (sortedMap.size() <= 0) {
      sortedMap.put("NA1", new Long(1));
      sortedMap.put("NA2", new Long(2));
    }

    int count = 5;
    for (Map.Entry<String, Long> entry : sortedMap.entrySet()) {
      Collection<Pair<String, Object>> row = new ArrayList<>();
      row.add(new Pair<String, Object>("Name", entry.getKey()));
      row.add(new Pair<String, Object>("Value", entry.getValue()));
      results.add(row);

      if (count-- <= 0) {
        break;
      }
    }
  }

  public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> unsortMap) {

    List<Map.Entry<K, V>> list =
      new LinkedList<Map.Entry<K, V>>(unsortMap.entrySet());

    Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
        return (o2.getValue()).compareTo(o1.getValue());
      }
    });

    Map<K, V> result = new LinkedHashMap<K, V>();
    for (Map.Entry<K, V> entry : list) {
      result.put(entry.getKey(), entry.getValue());
    }

    return result;
  }

  private static final Logger LOG = LoggerFactory.getLogger(TopNAccounts.class);
}
