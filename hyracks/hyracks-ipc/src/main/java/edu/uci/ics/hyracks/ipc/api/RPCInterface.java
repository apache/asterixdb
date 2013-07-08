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
package edu.uci.ics.hyracks.ipc.api;

import java.util.HashMap;
import java.util.Map;

public class RPCInterface implements IIPCI {
    private final Map<Long, Request> reqMap;

    public RPCInterface() {
        reqMap = new HashMap<Long, RPCInterface.Request>();
    }

    public Object call(IIPCHandle handle, Object request) throws Exception {
        Request req;
        synchronized (this) {
            req = new Request();
            long mid = handle.send(-1, request, null);
            reqMap.put(mid, req);
        }
        return req.getResponse();
    }

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload, Exception exception) {
        Request req;
        synchronized (this) {
            req = reqMap.remove(rmid);
        }
        assert req != null;
        if (exception != null) {
            req.setException(exception);
        } else {
            req.setResult(payload);
        }
    }

    private static class Request {
        private boolean pending;

        private Object result;

        private Exception exception;

        Request() {
            pending = true;
            result = null;
            exception = null;
        }

        synchronized void setResult(Object result) {
            this.pending = false;
            this.result = result;
            notifyAll();
        }

        synchronized void setException(Exception exception) {
            this.pending = false;
            this.exception = exception;
            notifyAll();
        }

        synchronized Object getResponse() throws Exception {
            while (pending) {
                wait();
            }
            if (exception != null) {
                throw exception;
            }
            return result;
        }
    }
}