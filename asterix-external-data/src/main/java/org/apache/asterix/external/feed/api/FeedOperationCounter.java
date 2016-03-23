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
package org.apache.asterix.external.feed.api;

import org.apache.asterix.external.feed.watch.FeedJobInfo;

public class FeedOperationCounter {
    private FeedJobInfo feedJobInfo;
    private int providersCount;
    private int jobsCount;
    private boolean failedIngestion = false;

    public FeedOperationCounter(int providersCount, int jobsCount) {
        this.providersCount = providersCount;
        this.jobsCount = jobsCount;
    }

    public int getProvidersCount() {
        return providersCount;
    }

    public void setProvidersCount(int providersCount) {
        this.providersCount = providersCount;
    }

    public int getJobsCount() {
        return jobsCount;
    }

    public void setJobsCount(int jobsCount) {
        this.jobsCount = jobsCount;
    }

    public boolean isFailedIngestion() {
        return failedIngestion;
    }

    public void setFailedIngestion(boolean failedIngestion) {
        this.failedIngestion = failedIngestion;
    }

    public FeedJobInfo getFeedJobInfo() {
        return feedJobInfo;
    }

    public void setFeedJobInfo(FeedJobInfo feedJobInfo) {
        this.feedJobInfo = feedJobInfo;
    }
}
