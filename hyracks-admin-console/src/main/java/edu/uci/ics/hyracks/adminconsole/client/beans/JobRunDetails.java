/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.adminconsole.client.beans;

import com.google.gwt.core.client.JavaScriptObject;
import com.google.gwt.core.client.JsArray;
import com.google.gwt.core.client.JsArrayInteger;
import com.google.gwt.core.client.JsArrayString;

public final class JobRunDetails extends JavaScriptObject {
    protected JobRunDetails() {
    }

    public native String getJobId()
    /*-{
         return this["job-id"];
    }-*/;

    public native String getApplicationName()
    /*-{
         return this["application-name"];
    }-*/;

    public native String getStatus()
    /*-{
         return this.status;
    }-*/;

    public native Long getCreateTime()
    /*-{
         return this["create-time"];
    }-*/;

    public native Long getStartTime()
    /*-{
         return this["start-time"];
    }-*/;

    public native Long getEndTime()
    /*-{
         return this["end-time"];
    }-*/;

    public native JsArray<ActivityClusterDetails> getActivityClusters()
    /*-{
         return this["activity-clusters"];
     }-*/;

    public static final class ActivityClusterDetails extends JavaScriptObject {
        protected ActivityClusterDetails() {
        }

        public native String getActivityClusterId()
        /*-{
             return this["activity-cluster-id"];
        }-*/;

        public native JsArrayString getActivities()
        /*-{
             return this.activities;
         }-*/;

        public native JsArrayString getDependents()
        /*-{
             return this.dependents;
         }-*/;

        public native JsArrayString getDependencies()
        /*-{
             return this.dependencies;
         }-*/;

        public native ActivityClusterPlan getActivityClusterPlan()
        /*-{
             return this.plan;
         }-*/;
    }

    public static final class ActivityClusterPlan extends JavaScriptObject {
        protected ActivityClusterPlan() {
        }

        public native JsArray<ActivityDetails> getActivities()
        /*-{
            return this["activities"];
        }-*/;

        public native JsArray<TaskClusterDetails> getTaskClusters()
        /*-{
            return this["task-clusters"];
        }-*/;
    }

    public static final class ActivityDetails extends JavaScriptObject {
        protected ActivityDetails() {
        }

        public native String getActivityId()
        /*-{
            return this["activity-id"];
        }-*/;

        public native Integer getPartitionCount()
        /*-{
            return this["partition-count"];
        }-*/;

        public native JsArrayInteger getInputPartitionCounts()
        /*-{
            return this["input-partition-counts"];
        }-*/;

        public native JsArrayInteger getOutputPartitionCounts()
        /*-{
            return this["output-partition-counts"];
        }-*/;

        public native JsArray<TaskDetails> getTasks()
        /*-{
            return this["tasks"];
        }-*/;
    }

    public static final class TaskDetails extends JavaScriptObject {
        protected TaskDetails() {
        }

        public native String getTaskId()
        /*-{
             return this["task-id"];
        }-*/;

        public native JsArrayString getDependents()
        /*-{
             return this.dependents;
         }-*/;

        public native JsArrayString getDependencies()
        /*-{
             return this.dependencies;
         }-*/;
    }

    public static final class TaskClusterDetails extends JavaScriptObject {
        protected TaskClusterDetails() {
        }

        public native String getTaskClusterId()
        /*-{
             return this["task-cluster-id"];
        }-*/;

        public native JsArrayString getTasks()
        /*-{
             return this.tasks;
         }-*/;

        public native JsArrayString getProducedPartitions()
        /*-{
             return this["produced-partitions"];
         }-*/;

        public native JsArrayString getRequiredPartitions()
        /*-{
             return this["required-partitions"];
         }-*/;
    }

    public static final class TaskClusterAttemptDetails extends JavaScriptObject {
        protected TaskClusterAttemptDetails() {
        }

        public native Integer getAttempt()
        /*-{
             return this["attempt"];
        }-*/;

        public native String getStatus()
        /*-{
             return this["status"];
        }-*/;

        public native JsArray<TaskAttemptDetails> getTaskAttempts()
        /*-{
             return this["task-attempts"];
        }-*/;
    }

    public static final class TaskAttemptDetails extends JavaScriptObject {
        protected TaskAttemptDetails() {
        }

        public native String getTaskId()
        /*-{
             return this["task-id"];
        }-*/;

        public native String getTaskAttemptId()
        /*-{
             return this["task-attempt-id"];
        }-*/;

        public native String getStatus()
        /*-{
             return this["status"];
        }-*/;

        public native String getNodeId()
        /*-{
             return this["node-id"];
        }-*/;

        public native ExceptionDetails getException()
        /*-{
             return this["exception"];
        }-*/;
    }

    public static final class ExceptionDetails extends JavaScriptObject {
        protected ExceptionDetails() {
        }

        public native String getExceptionClass()
        /*-{
             return this["exception-class"];
        }-*/;

        public native String getExceptionMessage()
        /*-{
             return this["exception-message"];
        }-*/;

        public native String getExceptionStacktrace()
        /*-{
             return this["exception-stacktrace"];
        }-*/;
    }
}