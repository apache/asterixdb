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
package edu.uci.ics.hyracks.control.common.work;

public class FutureValue<T> implements IResultCallback<T> {
    private boolean done;

    private T value;

    private Exception e;

    public FutureValue() {
        done = false;
        value = null;
        e = null;
    }

    @Override
    public synchronized void setValue(T value) {
        done = true;
        this.value = value;
        e = null;
        notifyAll();
    }

    @Override
    public synchronized void setException(Exception e) {
        done = true;
        this.e = e;
        value = null;
        notifyAll();
    }

    public synchronized void reset() {
        done = false;
        value = null;
        e = null;
        notifyAll();
    }

    public synchronized T get() throws Exception {
        while (!done) {
            wait();
        }
        if (e != null) {
            throw e;
        }
        return value;
    }
}