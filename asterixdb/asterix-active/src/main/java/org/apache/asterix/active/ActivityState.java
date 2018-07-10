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
package org.apache.asterix.active;

public enum ActivityState {
    /**
     * The starting state and a possible terminal state.
     */
    STOPPED,
    /**
     * An unexpected failure caused the activity to fail but recovery attempts will start taking place
     */
    TEMPORARILY_FAILED,
    /**
     * Attempting to recover from temporary failure.
     */
    RECOVERING,
    /**
     * During an attempt to start the activity
     */
    STARTING,
    /**
     * The activity was started but is being cancelled. Waiting for the job cancellation to complete
     */
    CANCELLING,
    /**
     * The activity has been started successfully and is running
     */
    RUNNING,
    /**
     * During an attempt to gracefully stop the activity
     */
    STOPPING,
    /**
     * During an attempt to gracefully suspend the activity
     */
    SUSPENDING,
    /**
     * The activitiy has been suspended successfully. Next state must be resuming
     */
    SUSPENDED,
    /**
     * During an attempt to restart the activity after suspension
     */
    RESUMING
}