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

import static org.apache.asterix.api.http.server.ServletConstants.HYRACKS_CONNECTION_ATTR;

import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.ipc.exceptions.IPCException;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AbstractQueryApiServlet extends AbstractServlet {
    private static final Logger LOGGER = LogManager.getLogger();
    protected final IApplicationContext appCtx;

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

    AbstractQueryApiServlet(IApplicationContext appCtx, ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        this.appCtx = appCtx;
    }

    protected IResultSet getResultSet() throws Exception { // NOSONAR
        try {
            return ServletUtil.getResultSet(getHyracksClientConnection(), appCtx, ctx);
        } catch (IPCException e) {
            LOGGER.log(Level.WARN, "Failed getting hyracks dataset connection. Resetting hyracks connection.", e);
            ctx.put(HYRACKS_CONNECTION_ATTR, appCtx.getHcc());
            return ServletUtil.getResultSet(getHyracksClientConnection(), appCtx, ctx);
        }
    }

    protected IHyracksClientConnection getHyracksClientConnection() throws Exception { // NOSONAR
        IHyracksClientConnection hcc = (IHyracksClientConnection) ctx.get(HYRACKS_CONNECTION_ATTR);
        if (hcc == null) {
            throw new RuntimeDataException(ErrorCode.PROPERTY_NOT_SET, HYRACKS_CONNECTION_ATTR);
        }
        return hcc;
    }
}
