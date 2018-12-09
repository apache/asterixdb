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

import static org.apache.asterix.api.http.server.ServletConstants.RESULTSET_ATTR;

import java.util.Map;

import org.apache.asterix.app.result.ResultReader;
import org.apache.asterix.common.api.IApplicationContext;
import org.apache.hyracks.api.client.IHyracksClientConnection;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.client.result.ResultSet;

public class ServletUtil {
    static IResultSet getResultSet(IHyracksClientConnection hcc, IApplicationContext appCtx,
            final Map<String, Object> ctx) throws Exception {
        IResultSet resultSet = (IResultSet) ctx.get(RESULTSET_ATTR);
        if (resultSet == null) {
            synchronized (ctx) {
                resultSet = (IResultSet) ctx.get(RESULTSET_ATTR);
                if (resultSet == null) {
                    resultSet = new ResultSet(hcc,
                            appCtx.getServiceContext().getControllerService().getNetworkSecurityManager()
                                    .getSocketChannelFactory(),
                            appCtx.getCompilerProperties().getFrameSize(), ResultReader.NUM_READERS);
                    ctx.put(RESULTSET_ATTR, resultSet);
                }
            }
        }
        return resultSet;
    }
}
