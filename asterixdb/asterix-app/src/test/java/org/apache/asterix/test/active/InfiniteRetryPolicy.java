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
package org.apache.asterix.test.active;

import org.apache.asterix.active.IActiveEntityEventsListener;
import org.apache.hyracks.util.IRetryPolicy;

public class InfiniteRetryPolicy implements IRetryPolicy {

    private final IActiveEntityEventsListener listener;

    public InfiniteRetryPolicy(IActiveEntityEventsListener listener) {
        this.listener = listener;
    }

    @Override
    public boolean retry(Throwable failure) {
        synchronized (listener) {
            try {
                listener.wait(5000); //NOSONAR this method is being called in a while loop
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

}
