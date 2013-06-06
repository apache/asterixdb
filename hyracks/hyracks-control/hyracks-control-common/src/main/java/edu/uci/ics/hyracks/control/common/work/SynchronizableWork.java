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

public abstract class SynchronizableWork extends AbstractWork {
    private boolean done;

    private Exception e;

    protected abstract void doRun() throws Exception;

    public void init() {
        done = false;
        e = null;
    }

    @Override
    public final void run() {
        try {
            doRun();
        } catch (Exception e) {
            this.e = e;
        } finally {
            synchronized (this) {
                done = true;
                notifyAll();
            }
        }
    }

    public final synchronized void sync() throws Exception {
        while (!done) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw e;
            }
        }
        if (e != null) {
            throw e;
        }
    }
}