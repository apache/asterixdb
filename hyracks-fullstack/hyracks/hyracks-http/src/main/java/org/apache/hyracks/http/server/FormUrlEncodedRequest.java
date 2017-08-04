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
package org.apache.hyracks.http.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.QueryStringDecoder;
import io.netty.handler.codec.http.multipart.Attribute;
import io.netty.handler.codec.http.multipart.HttpPostRequestDecoder;
import io.netty.handler.codec.http.multipart.InterfaceHttpData;
import io.netty.handler.codec.http.multipart.MixedAttribute;

public class FormUrlEncodedRequest extends BaseRequest implements IServletRequest {

    private final List<String> names;
    private final List<String> values;

    public static IServletRequest create(FullHttpRequest request) throws IOException {
        List<String> names = new ArrayList<>();
        List<String> values = new ArrayList<>();
        HttpPostRequestDecoder decoder = new HttpPostRequestDecoder(request);
        try {
            List<InterfaceHttpData> bodyHttpDatas = decoder.getBodyHttpDatas();
            for (InterfaceHttpData data : bodyHttpDatas) {
                if (data.getHttpDataType().equals(InterfaceHttpData.HttpDataType.Attribute)) {
                    Attribute attr = (MixedAttribute) data;
                    names.add(data.getName());
                    values.add(attr.getValue());
                }
            }
        } finally {
            decoder.destroy();
        }
        return new FormUrlEncodedRequest(request, new QueryStringDecoder(request.uri()).parameters(), names, values);
    }

    protected FormUrlEncodedRequest(FullHttpRequest request, Map<String, List<String>> parameters, List<String> names,
                                    List<String> values) {
        super(request, parameters);
        this.names = names;
        this.values = values;
    }

    @Override
    public String getParameter(CharSequence name) {
        for (int i = 0; i < names.size(); i++) {
            if (name.equals(names.get(i))) {
                return values.get(i);
            }
        }
        return HttpUtil.getParameter(parameters, name);
    }

    @Override
    public Set<String> getParameterNames() {
        HashSet<String> paramNames = new HashSet<>();
        paramNames.addAll(parameters.keySet());
        paramNames.addAll(names);
        return Collections.unmodifiableSet(paramNames);
    }

    @Override
    public Map<String, String> getParameters() {
        HashMap<String, String> paramMap = new HashMap<>();
        paramMap.putAll(super.getParameters());
        for (int i = 0; i < names.size(); i++) {
            paramMap.put(names.get(i), values.get(i));
        }

        return Collections.unmodifiableMap(paramMap);
    }
}
