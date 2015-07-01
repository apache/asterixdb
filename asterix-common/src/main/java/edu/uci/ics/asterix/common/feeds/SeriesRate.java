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

import java.util.Timer;
import java.util.TimerTask;

import edu.uci.ics.asterix.common.feeds.api.IFeedMetricCollector.MetricType;

public class SeriesRate extends Series {

    private static final long REFRESH_MEASUREMENT = 5000; // 5 seconds

    private int rate;
    private Timer timer;
    private RateComputingTask task;

    public SeriesRate() {
        super(MetricType.RATE);
        begin();
    }

    public int getRate() {
        return rate;
    }

    public synchronized void addValue(int value) {
        if (value < 0) {
            return;
        }
        runningSum += value;
    }

    public void begin() {
        if (timer == null) {
            timer = new Timer();
            task = new RateComputingTask(this);
            timer.scheduleAtFixedRate(task, 0, REFRESH_MEASUREMENT);
        }
    }

    public void end() {
        if (timer != null) {
            timer.cancel();
        }
    }

    public void reset() {
        rate = 0;
        if (task != null) {
            task.reset();
        }
    }

    private class RateComputingTask extends TimerTask {

        private int lastMeasured = 0;
        private final SeriesRate series;

        public RateComputingTask(SeriesRate series) {
            this.series = series;
        }

        @Override
        public void run() {
            int currentValue = series.getRunningSum();
            rate = (int) (((currentValue - lastMeasured) * 1000) / REFRESH_MEASUREMENT);
            lastMeasured = currentValue;
        }

        public void reset() {
            lastMeasured = 0;
        }
    }

}
