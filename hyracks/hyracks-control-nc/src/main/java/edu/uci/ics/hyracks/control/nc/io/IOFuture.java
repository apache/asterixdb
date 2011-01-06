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
package edu.uci.ics.hyracks.control.nc.io;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public final class IOFuture {
    private boolean complete;

    private Exception exception;

    public synchronized void reset() {
        complete = false;
        exception = null;
    }

    public synchronized void synchronize() throws HyracksDataException, InterruptedException {
        while (!complete) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw e;
            }
        }
        if (exception != null) {
            throw new HyracksDataException(exception);
        }
    }

    public synchronized boolean isComplete() {
        return complete;
    }

    public synchronized void notifySuccess() {
        complete = true;
        notifyAll();
    }

    public synchronized void notifyFailure(Exception e) {
        complete = true;
        exception = e;
        notifyAll();
    }
}