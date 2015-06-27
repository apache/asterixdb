/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.common.feeds;

import java.io.Serializable;
import java.util.Map;

/**
 * A utility class to access the configuration parameters of a feed ingestion policy.
 */
public class FeedPolicyAccessor implements Serializable {

    private static final long serialVersionUID = 1L;

    /** failure configuration **/
    /** continue feed ingestion after a soft (runtime) failure **/
    public static final String SOFT_FAILURE_CONTINUE = "soft.failure.continue";

    /** log failed tuple to an asterixdb dataset for future reference **/
    public static final String SOFT_FAILURE_LOG_DATA = "soft.failure.log.data";

    /** continue feed ingestion after loss of one or more machines (hardware failure) **/
    public static final String HARDWARE_FAILURE_CONTINUE = "hardware.failure.continue";

    /** auto-start a loser feed when the asterixdb instance is restarted **/
    public static final String CLUSTER_REBOOT_AUTO_RESTART = "cluster.reboot.auto.restart";

    /** framework provides guarantee that each received feed record will be processed through the ingestion pipeline at least once **/
    public static final String AT_LEAST_ONE_SEMANTICS = "atleast.once.semantics";

    /** flow control configuration **/
    /** spill excess tuples to disk if an operator cannot process incoming data at its arrival rate **/
    public static final String SPILL_TO_DISK_ON_CONGESTION = "spill.to.disk.on.congestion";

    /** the maximum size of data (tuples) that can be spilled to disk **/
    public static final String MAX_SPILL_SIZE_ON_DISK = "max.spill.size.on.disk";

    /** discard tuples altogether if an operator cannot process incoming data at its arrival rate **/
    public static final String DISCARD_ON_CONGESTION = "discard.on.congestion";

    /** maximum fraction of ingested data that can be discarded **/
    public static final String MAX_FRACTION_DISCARD = "max.fraction.discard";

    /** maximum end-to-end delay/latency in persisting a tuple through the feed ingestion pipeline **/
    public static final String MAX_DELAY_RECORD_PERSISTENCE = "max.delay.record.persistence";

    /** rate limit the inflow of tuples in accordance with the maximum capacity of the pipeline **/
    public static final String THROTTLING_ENABLED = "throttling.enabled";

    /** elasticity **/
    public static final String ELASTIC = "elastic";

    /** statistics **/
    public static final String TIME_TRACKING = "time.tracking";

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

    /** Failure recover/reporting **/

    public boolean logDataOnSoftFailure() {
        return getBooleanPropertyValue(SOFT_FAILURE_LOG_DATA, false);
    }

    public boolean continueOnSoftFailure() {
        return getBooleanPropertyValue(SOFT_FAILURE_CONTINUE, false);
    }

    public boolean continueOnHardwareFailure() {
        return getBooleanPropertyValue(HARDWARE_FAILURE_CONTINUE, false);
    }

    public boolean autoRestartOnClusterReboot() {
        return getBooleanPropertyValue(CLUSTER_REBOOT_AUTO_RESTART, false);
    }

    public boolean atleastOnceSemantics() {
        return getBooleanPropertyValue(AT_LEAST_ONE_SEMANTICS, false);
    }

    /** flow control **/
    public boolean spillToDiskOnCongestion() {
        return getBooleanPropertyValue(SPILL_TO_DISK_ON_CONGESTION, false);
    }

    public boolean discardOnCongestion() {
        return getMaxFractionDiscard() > 0;
    }

    public boolean throttlingEnabled() {
        return getBooleanPropertyValue(THROTTLING_ENABLED, false);
    }

    public long getMaxSpillOnDisk() {
        return getLongPropertyValue(MAX_SPILL_SIZE_ON_DISK, NO_LIMIT);
    }

    public float getMaxFractionDiscard() {
        return getFloatPropertyValue(MAX_FRACTION_DISCARD, 0);
    }

    public long getMaxDelayRecordPersistence() {
        return getLongPropertyValue(MAX_DELAY_RECORD_PERSISTENCE, Long.MAX_VALUE);
    }

    /** Elasticity **/
    public boolean isElastic() {
        return getBooleanPropertyValue(ELASTIC, false);
    }

    /** Statistics **/
    public boolean isTimeTrackingEnabled() {
        return getBooleanPropertyValue(TIME_TRACKING, false);
    }

    /** Logging of statistics **/
    public boolean isLoggingStatisticsEnabled() {
        return getBooleanPropertyValue(LOGGING_STATISTICS, false);
    }

    private boolean getBooleanPropertyValue(String key, boolean defValue) {
        String v = feedPolicy.get(key);
        return v == null ? false : Boolean.valueOf(v);
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