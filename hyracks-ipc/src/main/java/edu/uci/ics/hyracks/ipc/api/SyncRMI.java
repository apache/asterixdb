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
package edu.uci.ics.hyracks.ipc.api;

public final class SyncRMI implements IResponseCallback {
    private boolean pending;

    private Object response;

    private Exception exception;

    public SyncRMI() {
    }

    @Override
    public synchronized void callback(IIPCHandle handle, Object response, Exception exception) {
        pending = false;
        this.response = response;
        this.exception = exception;
        notifyAll();
    }

    public synchronized Object call(IIPCHandle handle, Object request) throws Exception {
        pending = true;
        response = null;
        exception = null;
        handle.send(request, this);
        while (pending) {
            wait();
        }
        if (exception != null) {
            throw exception;
        }
        return response;
    }
}