/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.genericdemo.enginedata;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.netlet.util.DTThrowable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;
import java.util.Map.Entry;
import javax.validation.constraints.Min;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import org.codehaus.jackson.JsonNode;

/**
 * Generates sales events data and sends them out as JSON encoded byte arrays.
 * <p>
 * Sales events are JSON string encoded as byte arrays. All id's are expected to be positive integers, and default to 1.
 * Transaction amounts are double values with two decimal places. Timestamp is unix epoch in milliseconds.
 * Product categories are not assigned by default. They are expected to be added by the Enrichment operator, but can be
 * enabled with addProductCategory override.
 *
 * Example Sales Event
 *
 * {
 * "productId": 1,
 * "customerId": 12345,
 * "productCategory": 0,
 * "regionId": 2,
 * "channelId": 3,
 * "sales": 107.99,
 * "tax": 7.99,
 * "discount": 15.73,
 * "timestamp": 1412897574000
 * }
 *
 * @displayName JSON Sales Event Generator
 * @category Test Bench
 * @tags input, generator, json
 *
 * @since 2.0.0
 */
public class EngineDataGenerator implements InputOperator
{
  public static final String KEY_ERRORCODE = "errorCode";
  public static final String KEY_MODEL = "model";
  public static final String KEY_GEAR = "gear";
  public static final String KEY_TEMPERATURE = "temperature";
  public static final String KEY_RPM = "rpm";
  public static final String KEY_SPEED = "speed";

  private final double speedTiers[] = new double[] {0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0, 95.0, 100.0};
  private final String models[] = new String[] {"1.2/1.5 L B38", "2.0 L N20", "1.6 L N20", "1.6 L N13", "2.0 L N26", "2.0 L B48", "3.0 L B58", "BMW S63"};
  private final int gears[] = new int[] {1, 2, 3, 4, 5, 6, 7};
  private final double rpmTiers[] = new double[] {500.0, 1000.0, 1500.0, 2000.0, 2500.0, 3000.0, 3500.0, 4000.0, 4500.0, 5000.0, 5500.0, 6000.0, 6500.0, 7000.0, 7500.0, 8000.0};
  private final double temperatureTiers[] = new double[] {-70, -6, -60, -55, -50, -45, -40, -35, -30, -25, -20, -15, -10, -5, 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95, 100, 105, 110, 115, 120};
  private final List<String> errorCodes = new ArrayList<>();
  private transient StringBuilder sb = new StringBuilder();

  // Limit number of emitted tuples per window
  @Min(0)
  private long maxTuplesPerWindow = 300;

  // Maximum sales of deviation below the maximum tuples per window
  @Min(0)
  private int tuplesPerWindowDeviation = 200;

  // Number of windows to maintain the same deviation before selecting another
  @Min(1)
  private int tuplesRateCycle = 40;

  /**
   * Outputs sales event in JSON format as a byte array
   */
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  private static final ObjectMapper mapper = new ObjectMapper().setSerializationInclusion(JsonSerialize.Inclusion.NON_NULL);
  private final Random random = new Random();

  private long tuplesCounter = 0;
  private long tuplesPerCurrentWindow = maxTuplesPerWindow;
  @Override
  public void beginWindow(long windowId)
  {
    tuplesCounter = 0;
    // Generate new output rate after tuplesRateCycle windows ONLY if tuplesPerWindowDeviation is non-zero
    if (windowId % tuplesRateCycle == 0 && tuplesPerWindowDeviation > 0) {
      tuplesPerCurrentWindow = maxTuplesPerWindow - random.nextInt(tuplesPerWindowDeviation);
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    loadErrorCodes();
    tuplesPerCurrentWindow = maxTuplesPerWindow;
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void emitTuples()
  {
    while (tuplesCounter++ < tuplesPerCurrentWindow) {
      try {

        EngineDataEvent engineDataEvent = generateEngineDataEvent();
        this.output.emit(convertToCsv(engineDataEvent));
      }
      catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private EngineDataEvent generateEngineDataEvent() throws Exception
  {
    EngineDataEvent engineDataEvent = new EngineDataEvent();

    engineDataEvent.speed = speedTiers[random.nextInt(speedTiers.length)];
    engineDataEvent.rpm = rpmTiers[random.nextInt(rpmTiers.length)];
    engineDataEvent.temperature = temperatureTiers[random.nextInt(temperatureTiers.length)];
    engineDataEvent.gear = gears[random.nextInt(gears.length)];
    engineDataEvent.model = models[random.nextInt(models.length)];
    engineDataEvent.errorCode = errorCodes.get(random.nextInt(errorCodes.size()));
    engineDataEvent.time = System.currentTimeMillis();

    return engineDataEvent;
  }

  public long getMaxTuplesPerWindow()
  {
    return maxTuplesPerWindow;
  }

  public void setMaxTuplesPerWindow(long maxTuplesPerWindow)
  {
    this.maxTuplesPerWindow = maxTuplesPerWindow;
  }

  private void loadErrorCodes()
  {
    try {
      InputStream is = this.getClass().getClassLoader().getResourceAsStream("ErrorCodes.txt");
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      String line;
      while ((line = reader.readLine()) != null) {
        JsonNode tree = mapper.readTree(line);
        Iterator<Entry<String, JsonNode>> fields = tree.getFields();
        String key = fields.next().getValue().asText();
        errorCodes.add(key);
      }
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
  }

  private byte[] convertToCsv(EngineDataEvent engineDataEvent)
  {
    sb.setLength(0);
    sb.append(engineDataEvent.time).append(",");
    sb.append(engineDataEvent.errorCode).append(",");
    sb.append(engineDataEvent.model).append(",");
    sb.append(engineDataEvent.gear).append(",");
    sb.append(engineDataEvent.temperature).append(",");
    sb.append(engineDataEvent.speed).append(",");
    sb.append(engineDataEvent.rpm);

    return sb.toString().getBytes();
  }
}

