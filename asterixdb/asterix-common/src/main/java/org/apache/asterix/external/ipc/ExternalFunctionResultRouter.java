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
package org.apache.asterix.external.ipc;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.Message;

public class ExternalFunctionResultRouter implements IIPCI {

    AtomicLong maxId = new AtomicLong(0);
    ConcurrentHashMap<Long, MutableObject<ByteBuffer>> activeClients = new ConcurrentHashMap<>();
    ConcurrentHashMap<Long, Exception> exceptionInbox = new ConcurrentHashMap<>();
    private static int MAX_BUF_SIZE = 32 * 1024 * 1024; //32MB

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        int rewind = handle.getAttachmentLen();
        ByteBuffer buf = (ByteBuffer) payload;
        int end = buf.position();
        buf.position(end - rewind);
        ByteBuffer copyTo = activeClients.get(rmid).getValue();
        if (copyTo.capacity() < handle.getAttachmentLen()) {
            int nextSize = closestPow2(handle.getAttachmentLen());
            if (nextSize > MAX_BUF_SIZE) {
                onError(handle, mid, rmid, HyracksException.create(ErrorCode.RECORD_IS_TOO_LARGE));
                return;
            }
            copyTo = ByteBuffer.allocate(nextSize);
            activeClients.get(rmid).setValue(copyTo);
        }
        copyTo.position(0);
        System.arraycopy(buf.array(), buf.position() + buf.arrayOffset(), copyTo.array(), copyTo.arrayOffset(),
                handle.getAttachmentLen());
        synchronized (copyTo) {
            copyTo.limit(handle.getAttachmentLen() + 1);
            copyTo.notify();
        }
        buf.position(end);
    }

    @Override
    public void onError(IIPCHandle handle, long mid, long rmid, Exception exception) {
        exceptionInbox.put(rmid, exception);
        ByteBuffer route = activeClients.get(rmid).getValue();
        synchronized (route) {
            route.notify();
        }
    }

    public Long insertRoute(ByteBuffer buf) {
        Long id = maxId.incrementAndGet();
        activeClients.put(id, new MutableObject<>(buf));
        return id;
    }

    public Exception getException(Long id) {
        return exceptionInbox.remove(id);
    }

    public boolean hasException(long id) {
        return exceptionInbox.get(id) == null;
    }

    public void removeRoute(Long id) {
        activeClients.remove(id);
        exceptionInbox.remove(id);
    }

    public static int closestPow2(int n) {
        return (int) Math.pow(2, Math.ceil(Math.log(n) / Math.log(2)));
    }

    public static class NoOpNoSerJustDe implements IPayloadSerializerDeserializer {

        private static byte[] noop = new byte[] { (byte) 0 };

        @Override
        public Object deserializeObject(ByteBuffer buffer, int length, byte flag) throws Exception {
            if (flag == Message.INITIAL_REQ) {
                return new JavaSerializationBasedPayloadSerializerDeserializer().deserializeObject(buffer, length,
                        flag);
            }
            return buffer;
        }

        @Override
        public Exception deserializeException(ByteBuffer buffer, int length) throws Exception {
            return null;
        }

        @Override
        public byte[] serializeObject(Object object) throws Exception {
            return noop;
        }

        @Override
        public byte[] serializeException(Exception object) throws Exception {
            return noop;
        }
    }
}
