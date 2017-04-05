/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.feed.policy;

import java.io.Serializable;
import java.util.Map;

/**
 * A utility class to access the configuration parameters of a feed ingestion policy.
 */
public class FeedPolicyAccessor implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * --------------------------
     * flow control configuration
     * --------------------------
     **/

    /** enable buffering in feeds **/
    public static final String FLOWCONTROL_ENABLED = "flowcontrol.enabled";

    /** spill excess tuples to disk if an operator cannot process incoming data at its arrival rate **/
    public static final String SPILL_TO_DISK_ON_CONGESTION = "spill.to.disk.on.congestion";

    /** the maximum size of data (tuples) that can be spilled to disk **/
    public static final String MAX_SPILL_SIZE_ON_DISK = "max.spill.size.on.disk";

    /** discard tuples altogether if an operator cannot process incoming data at its arrival rate **/
    public static final String DISCARD_ON_CONGESTION = "discard.on.congestion";

    /** maximum fraction of ingested data that can be discarded **/
    public static final String MAX_FRACTION_DISCARD = "max.fraction.discard";

    /** elasticity **/
    public static final String ELASTIC = "elastic";

    /** logging of statistics **/
    public static final String LOGGING_STATISTICS = "logging.statistics";

    public static final long NO_LIMIT = -1;

    private Map<String, String> feedPolicy;

    public Map<String, String> getFeedPolicy() {
        return feedPolicy;
    }

    public FeedPolicyAccessor(Map<String, String> feedPolicy) {
        this.feedPolicy = feedPolicy;
    }

    public void reset(Map<String, String> feedPolicy) {
        this.feedPolicy = feedPolicy;
    }

    /** flow control **/
    public boolean flowControlEnabled() {
        return getBooleanPropertyValue(FLOWCONTROL_ENABLED, false);
    }

    public boolean spillToDiskOnCongestion() {
        return getBooleanPropertyValue(SPILL_TO_DISK_ON_CONGESTION, false);
    }

    public boolean discardOnCongestion() {
        return getMaxFractionDiscard() > 0;
    }

    public long getMaxSpillOnDisk() {
        return getLongPropertyValue(MAX_SPILL_SIZE_ON_DISK, NO_LIMIT);
    }

    public float getMaxFractionDiscard() {
        return getFloatPropertyValue(MAX_FRACTION_DISCARD, 0);
    }

    private boolean getBooleanPropertyValue(String key, boolean defValue) {
        String v = feedPolicy.get(key);
        return v == null ? defValue : Boolean.valueOf(v);
    }

    private long getLongPropertyValue(String key, long defValue) {
        String v = feedPolicy.get(key);
        return v != null ? Long.parseLong(v) : defValue;
    }

    private float getFloatPropertyValue(String key, float defValue) {
        String v = feedPolicy.get(key);
        return v != null ? Float.parseFloat(v) : defValue;
    }

}
