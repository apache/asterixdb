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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.dataset.IHyracksDataset;
import org.apache.hyracks.client.dataset.HyracksDataset;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.AbstractServlet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class AbstractQueryApiServlet extends AbstractServlet {

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

    AbstractQueryApiServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    protected IHyracksDataset getHyracksDataset() throws Exception { // NOSONAR
        synchronized (ctx) {
            IHyracksDataset hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
            if (hds == null) {
                hds = (IHyracksDataset) ctx.get(HYRACKS_DATASET_ATTR);
                if (hds == null) {
                    hds = new HyracksDataset(getHyracksClientConnection(), ResultReader.FRAME_SIZE,
                            ResultReader.NUM_READERS);
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

    protected static JsonNode parseHandle(ObjectMapper om, String strHandle, Logger logger) throws IOException {
        if (strHandle == null) {
            logger.log(Level.WARNING, "No handle provided");
        } else {
            try {
                JsonNode handleObj = om.readTree(strHandle);
                return handleObj.get("handle");
            } catch (JsonProcessingException e) { // NOSONAR
                logger.log(Level.WARNING, "Invalid handle: \"" + strHandle + "\"");
            }
        }
        return null;
    }

    protected static UUID printRequestId(PrintWriter pw) {
        UUID requestId = UUID.randomUUID();
        printField(pw, ResultFields.REQUEST_ID.str(), requestId.toString());
        return requestId;
    }

    protected static void printStatus(PrintWriter pw, ResultStatus rs) {
        printField(pw, ResultFields.STATUS.str(), rs.str());
    }

    protected static void printHandle(PrintWriter pw, String handle) {
        printField(pw, ResultFields.HANDLE.str(), handle);
    }

    protected static void printField(PrintWriter pw, String name, String value) {
        printField(pw, name, value, true);
    }

    protected static void printField(PrintWriter pw, String name, String value, boolean comma) {
        printFieldInternal(pw, name, "\"" + value + "\"", comma);
    }

    protected static void printField(PrintWriter pw, String name, long value, boolean comma) {
        printFieldInternal(pw, name, String.valueOf(value), comma);
    }

    protected static void printFieldInternal(PrintWriter pw, String name, String value, boolean comma) {
        pw.print("\t\"");
        pw.print(name);
        pw.print("\": ");
        pw.print(value);
        if (comma) {
            pw.print(',');
        }
        pw.print('\n');
    }

}
