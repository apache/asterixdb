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
package edu.uci.ics.asterix.common.feeds.api;

/**
 * Provides a callback mechanism that in invoked for events related to
 * the execution of a feed management task.
 */
public interface IFeedWorkEventListener {

    /**
     * A call back that is invoked after successful completion of a feed
     * management task.
     */
    public void workCompleted(IFeedWork work);

    /**
     * A call back that is invokved after a failed execution of a feed
     * management task.
     * 
     * @param e
     *            exception encountered during execution of the task.
     */
    public void workFailed(IFeedWork work, Exception e);
}
