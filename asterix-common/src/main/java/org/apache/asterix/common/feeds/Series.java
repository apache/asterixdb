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

import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;

public abstract class Series {

    protected final MetricType type;
    protected int runningSum;

    public Series(MetricType type) {
        this.type = type;
    }

    public abstract void addValue(int value);

    public int getRunningSum() {
        return runningSum;
    }

    public MetricType getType() {
        return type;
    }

    public abstract void reset();

}
