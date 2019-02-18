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
package org.apache.asterix.translator;

import java.util.UUID;

import org.apache.asterix.common.api.IClientRequest;
import org.apache.asterix.common.api.ICommonRequestParameters;
import org.apache.asterix.common.api.IReceptionist;
import org.apache.asterix.common.api.IRequestReference;
import org.apache.asterix.common.api.ISchedulableClientRequest;
import org.apache.asterix.common.api.RequestReference;
import org.apache.http.HttpHeaders;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.util.NetworkUtil;

public class Receptionist implements IReceptionist {

    private final String node;

    public Receptionist(String node) {
        this.node = node;
    }

    @Override
    public IRequestReference welcome(IServletRequest request) {
        final String uuid = UUID.randomUUID().toString();
        final RequestReference ref = RequestReference.of(uuid, node, System.currentTimeMillis());
        ref.setUserAgent(request.getHeader(HttpHeaders.USER_AGENT));
        ref.setRemoteAddr(NetworkUtil.toHostPort(request.getRemoteAddress()));
        return ref;
    }

    @Override
    public IClientRequest requestReceived(ICommonRequestParameters requestParameters) throws HyracksDataException {
        return new ClientRequest(requestParameters);
    }

    @Override
    public void ensureSchedulable(ISchedulableClientRequest schedulableRequest) throws HyracksDataException {
        // currently we don't have any restrictions
    }
}
