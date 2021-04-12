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

import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksException;
import org.apache.hyracks.ipc.api.IIPCHandle;
import org.apache.hyracks.ipc.api.IIPCI;
import org.apache.hyracks.ipc.api.IPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.JavaSerializationBasedPayloadSerializerDeserializer;
import org.apache.hyracks.ipc.impl.Message;

public class ExternalFunctionResultRouter implements IIPCI {

    private final AtomicLong maxId = new AtomicLong(0);
    private final ConcurrentHashMap<Long, Pair<ByteBuffer, Exception>> activeClients = new ConcurrentHashMap<>();
    private static int MAX_BUF_SIZE = 32 * 1024 * 1024; //32MB

    @Override
    public void deliverIncomingMessage(IIPCHandle handle, long mid, long rmid, Object payload) {
        int rewind = handle.getAttachmentLen();
        ByteBuffer buf = (ByteBuffer) payload;
        int end = buf.position();
        buf.position(end - rewind);
        Pair<ByteBuffer, Exception> route = activeClients.get(rmid);
        ByteBuffer copyTo = route.getFirst();
        if (copyTo.capacity() < handle.getAttachmentLen()) {
            int nextSize = closestPow2(handle.getAttachmentLen());
            if (nextSize > MAX_BUF_SIZE) {
                onError(handle, mid, rmid, HyracksException.create(ErrorCode.RECORD_IS_TOO_LARGE));
                return;
            }
            copyTo = ByteBuffer.allocate(nextSize);
            route.setFirst(copyTo);
        }
        copyTo.position(0);
        System.arraycopy(buf.array(), buf.position() + buf.arrayOffset(), copyTo.array(), copyTo.arrayOffset(),
                handle.getAttachmentLen());
        synchronized (route) {
            copyTo.limit(handle.getAttachmentLen() + 1);
            route.notifyAll();
        }
        buf.position(end);
    }

    @Override
    public void onError(IIPCHandle handle, long mid, long rmid, Exception exception) {
        Pair<ByteBuffer, Exception> route = activeClients.get(rmid);
        synchronized (route) {
            route.setSecond(exception);
            route.notifyAll();
        }
    }

    public Pair<Long, Pair<ByteBuffer, Exception>> insertRoute(ByteBuffer buf) {
        Long id = maxId.getAndIncrement();
        Pair<ByteBuffer, Exception> bufferHolder = new Pair<>(buf, null);
        activeClients.put(id, bufferHolder);
        return new Pair<>(id, bufferHolder);
    }

    public Exception getAndRemoveException(Long id) {
        Pair<ByteBuffer, Exception> route = activeClients.get(id);
        Exception e = route.getSecond();
        route.setSecond(null);
        return e;
    }

    public void removeRoute(Long id) {
        activeClients.remove(id);
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
