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
package edu.uci.ics.asterix.event.management;

import java.io.Serializable;

public class EventTaskReport implements Serializable {

    private static final long serialVersionUID = 1L;
    private final EventTask task;
    private final boolean success;
    private final Exception e;

    public EventTaskReport(EventTask task, boolean success, Exception e) {
        this.task = task;
        this.success = success;
        this.e = e;
    }

    public EventTaskReport(EventTask task) {
        this.task = task;
        this.success = true;
        this.e = null;
    }

    public Exception getException() {
        return e;
    }

    public EventTask getTask() {
        return task;
    }

    public boolean isSuccess() {
        return success;
    }

}
