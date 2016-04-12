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
package org.apache.hyracks.control.common.work;

import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.exceptions.IPCException;

public class IPCResponder<T> implements IResultCallback<T> {
    private final IIPCHandle handle;

    private final long rmid;

    public IPCResponder(IIPCHandle handle, long rmid) {
        this.handle = handle;
        this.rmid = rmid;
    }

    @Override
    public void setValue(T result) {
        try {
            handle.send(rmid, result, null);
        } catch (IPCException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setException(Exception e) {
        try {
            handle.send(rmid, null, e);
        } catch (IPCException e1) {
            e1.printStackTrace();
        }
    }
}
