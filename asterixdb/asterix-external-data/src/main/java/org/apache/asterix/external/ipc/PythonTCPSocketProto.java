/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.ipc;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.external.api.IExternalLangIPCProto;
import org.apache.hyracks.algebricks.common.utils.Pair;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class PythonTCPSocketProto extends AbstractPythonIPCProto
        implements org.apache.asterix.external.api.IExternalLangIPCProto {

    private final ExternalFunctionResultRouter router;
    private final Process proc;

    public PythonTCPSocketProto(OutputStream sockOut, ExternalFunctionResultRouter router, Process pythonProc) {
        super(sockOut);
        this.router = router;
        this.proc = pythonProc;
    }

    @Override
    public void start() {
        Pair<Long, Pair<ByteBuffer, Exception>> keyAndBufferBox = router.insertRoute(recvBuffer);
        this.routeId = keyAndBufferBox.getFirst();
        this.bufferBox = keyAndBufferBox.getSecond();
    }

    @Override
    public void quit() throws HyracksDataException {
        messageBuilder.quit();
        router.removeRoute(routeId);
    }

    @Override
    public void receiveMsg() throws IOException, AsterixException {
        Exception except;
        try {
            synchronized (bufferBox) {
                while ((bufferBox.getFirst().limit() == 0 || bufferBox.getSecond() != null) && proc.isAlive()) {
                    bufferBox.wait(100);
                }
            }
            except = router.getAndRemoveException(routeId);
            if (!proc.isAlive()) {
                except = new IOException("Python process exited with code: " + proc.exitValue());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AsterixException(ErrorCode.EXTERNAL_UDF_EXCEPTION, e);
        }
        if (except != null) {
            throw new AsterixException(except);
        }
        if (bufferBox.getFirst() != recvBuffer) {
            recvBuffer = bufferBox.getFirst();
        }
        messageBuilder.readHead(recvBuffer);
        if (messageBuilder.type == MessageType.ERROR) {
            unpackerInput.reset(recvBuffer.array(), recvBuffer.position() + recvBuffer.arrayOffset(),
                    recvBuffer.remaining());
            unpacker.reset(unpackerInput);
            throw new AsterixException(unpacker.unpackString());
        }
    }
}
