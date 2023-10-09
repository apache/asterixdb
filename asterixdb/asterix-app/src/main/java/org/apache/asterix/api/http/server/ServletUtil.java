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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.api.IApplicationContext;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.common.metadata.Namespace;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.result.IResultSet;
import org.apache.hyracks.http.api.IServletRequest;

public class ServletUtil {
    static IResultSet getResultSet(IApplicationContext appCtx, final Map<String, Object> ctx) throws Exception {
        IResultSet resultSet = (IResultSet) ctx.get(RESULTSET_ATTR);
        if (resultSet == null) {
            synchronized (ctx) {
                resultSet = (IResultSet) ctx.get(RESULTSET_ATTR);
                if (resultSet == null) {
                    resultSet = appCtx.getResultSet();
                    ctx.put(RESULTSET_ATTR, resultSet);
                }
            }
        }
        return resultSet;
    }

    public static DataverseName getDataverseName(IServletRequest request, String dataverseParameterName)
            throws AlgebricksException {
        List<String> values = request.getParameterValues(dataverseParameterName);
        return !values.isEmpty() ? DataverseName.create(values) : null;
    }

    public static Namespace getNamespace(IApplicationContext appCtx, IServletRequest request,
            String dataverseParameterName) throws AlgebricksException {
        List<String> values = request.getParameterValues(dataverseParameterName);
        return !values.isEmpty() ? appCtx.getNamespaceResolver().resolve(values) : null;
    }

    public static String decodeUriSegment(String uriSegment) {
        try {
            return new String(URLCodec.decodeUrl(uriSegment.getBytes(StandardCharsets.US_ASCII)),
                    StandardCharsets.UTF_8);
        } catch (DecoderException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
