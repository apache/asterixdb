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
package org.apache.asterix.common.api;

import org.apache.hyracks.util.JSONUtil;

import com.fasterxml.jackson.databind.node.ObjectNode;

public class RequestReference implements IRequestReference {

    private static final long serialVersionUID = 1L;
    private String uuid;
    private String node;
    private long time;
    private String userAgent;
    private String remoteAddr;

    private RequestReference(String uuid, String node, long time) {
        this.uuid = uuid;
        this.node = node;
        this.time = time;
    }

    public static RequestReference of(String uuid, String node, long time) {
        return new RequestReference(uuid, node, time);
    }

    @Override
    public String getUuid() {
        return uuid;
    }

    @Override
    public long getTime() {
        return time;
    }

    public String getNode() {
        return node;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public void setRemoteAddr(String remoteAddr) {
        this.remoteAddr = remoteAddr;
    }

    public String getRemoteAddr() {
        return remoteAddr;
    }

    @Override
    public String toString() {
        final ObjectNode object = JSONUtil.createObject();
        object.put("uuid", uuid);
        object.put("node", node);
        object.put("time", time);
        object.put("userAgent", userAgent);
        object.put("remoteAddr", remoteAddr);
        return JSONUtil.convertNodeOrThrow(object);
    }
}
