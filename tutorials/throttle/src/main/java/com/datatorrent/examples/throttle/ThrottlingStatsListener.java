package com.datatorrent.examples.throttle;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.Operator;
import com.datatorrent.api.StatsListener;

/**
 * Created by pramod on 9/27/16.
 */
public class ThrottlingStatsListener implements StatsListener, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(ThrottlingStatsListener.class);

    // The current window ids of the different operators that the stats listener is listening to stats for
    Map<Integer, Long> currentWindowIds = Maps.newHashMap();
    // Slowdown input if the window difference between operators increases beyond this value
    long maxThreshold = 100;
    // restore input operator to normal speed if the window difference falls below this threshold
    long minThreshold = 100;;
    boolean normalState = true;

    @Override
    public Response processStats(BatchedOperatorStats batchedOperatorStats)
    {
        Response response = new Response();
        int operatorId = batchedOperatorStats.getOperatorId();
        long windowId = batchedOperatorStats.getCurrentWindowId();
        currentWindowIds.put(operatorId, windowId);

        // Find min and max window to compute difference
        long minWindow = Long.MAX_VALUE;
        long maxWindow = Long.MIN_VALUE;
        for (Long value : currentWindowIds.values()) {
            if (value < minWindow) minWindow = value;
            if (value > maxWindow) maxWindow = value;
        }
        logger.debug("Operator {} min window {} max window {}", operatorId, minWindow, maxWindow);

        if (normalState && ((maxWindow - minWindow) > maxThreshold)) {
            // In future send the request here which will happen only on a state change
            /*
            // Send request to operator to slow down
            logger.info("Sending suspend request");
            List<OperatorRequest> operatorRequests = new ArrayList<OperatorRequest>();
            operatorRequests.add(new InputSlowdownRequest());
            response.operatorRequests = operatorRequests;
            */
            logger.info("Setting suspend");
            normalState = false;
        } else if (!normalState && ((maxWindow - minWindow) <= minThreshold)) {
            // In future send the request here which will happen only on a state change
            /*
            // Send request to operator to get back to normal
            logger.info("Sending normal request");
            List<OperatorRequest> operatorRequests = new ArrayList<OperatorRequest>();
            operatorRequests.add(new InputNormalRequest());
            response.operatorRequests = operatorRequests;
            */
            logger.info("Setting normal");
            normalState = true;
        }

        if (!normalState) {
            // Send request to operator to slow down
            List<OperatorRequest> operatorRequests = new ArrayList<OperatorRequest>();
            operatorRequests.add(new InputSlowdownRequest());
            response.operatorRequests = operatorRequests;
        } else {
            // Send request to operator to get back to normal
            List<OperatorRequest> operatorRequests = new ArrayList<OperatorRequest>();
            operatorRequests.add(new InputNormalRequest());
            response.operatorRequests = operatorRequests;
        }

        return response;
    }

    // This runs on the operator side
    public static class InputSlowdownRequest implements OperatorRequest, Serializable
    {
        private static final Logger logger = LoggerFactory.getLogger(InputSlowdownRequest.class);

        @Override
        public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
        {
            logger.debug("Received slowdown operator {} operatorId {} windowId {}", operator, operatorId, windowId);
            if (operator instanceof RandomNumberGenerator) {
                RandomNumberGenerator generator = (RandomNumberGenerator)operator;
                generator.suspend();
            }
            return new InputOperatorResponse();
        }
    }

    public static class InputNormalRequest implements OperatorRequest, Serializable
    {
        private static final Logger logger = LoggerFactory.getLogger(InputNormalRequest.class);

        @Override
        public OperatorResponse execute(Operator operator, int operatorId, long windowId) throws IOException
        {
            logger.debug("Received normal operator {} operatorId {} windowId {}", operator, operatorId, windowId);
            if (operator instanceof RandomNumberGenerator) {
                RandomNumberGenerator generator = (RandomNumberGenerator)operator;
                generator.normal();
            }
            return new InputOperatorResponse();
        }
    }

    public static class InputOperatorResponse implements OperatorResponse, Serializable
    {

        @Override
        public Object getResponseId() {
            return 1;
        }

        @Override
        public Object getResponse() {
            return "";
        }
    }

    public long getMaxThreshold()
    {
        return maxThreshold;
    }

    public void setMaxThreshold(long maxThreshold)
    {
        this.maxThreshold = maxThreshold;
    }

    public long getMinThreshold()
    {
        return minThreshold;
    }

    public void setMinThreshold(long minThreshold)
    {
        this.minThreshold = minThreshold;
    }
}
