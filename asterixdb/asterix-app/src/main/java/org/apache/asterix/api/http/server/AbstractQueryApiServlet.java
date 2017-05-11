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
package org.apache.asterix.api.http.server;

import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_CONNECTION_ATTR;
import static org.apache.asterix.api.http.servlet.ServletConstants.HYRACKS_DATASET_ATTR;

import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.http.server.AbstractServlet;

public class AbstractQueryApiServlet extends AbstractServlet {
    protected final IApplicationContext appCtx;

    public enum ResultFields {
        REQUEST_ID("requestID"),
        CLIENT_ID("clientContextID"),
        SIGNATURE("signature"),
        TYPE("type"),
        STATUS("status"),
        RESULTS("results"),
        HANDLE("handle"),
        ERRORS("errors"),
        METRICS("metrics");

        private final String str;

        ResultFields(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum ResultStatus {
        RUNNING("running"),
        SUCCESS("success"),
        TIMEOUT("timeout"),
        FAILED("failed"),
        FATAL("fatal");

        private final String str;

        ResultStatus(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    public enum ErrorField {
        CODE("code"),
        MSG("msg"),
        STACK("stack");

        private final String str;

        ErrorField(String str) {
            this.str = str;
        }

        public String str() {
            return str;
        }
    }

    AbstractQueryApiServlet(IApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    protected IHyracksDataset getHyracksDataset() throws Exception { // NOSONAR
        synchronized (ctx) {
            IHyracksDataset hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
            if (hds == null) {
                hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(getHyracksClientConnection(),
                            appCtx.getCompilerProperties().getFrameSize(), ResultReader.NUM_READERS);
                    ctx.put(HYRACKS_DATASET_ATTR, hds);
                }
            }
            return hds;
        }
    }

    protected IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        synchronized (ctx) {
            final IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
            if (hcc == null) {
                throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
            }
            return hcc;
        }
    }

    protected static UUID printRequestId(PrintWriter pw) {
        UUID requestId = UUID.randomUUID();
        ResultUtil.printField(pw, ResultFields.REQUEST_ID.str(), requestId.toString());
        return requestId;
    }

    protected static void printHandle(PrintWriter pw, String handle, boolean comma) {
        ResultUtil.printField(pw, ResultFields.HANDLE.str(), handle, comma);
    }

}
