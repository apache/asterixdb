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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A response to an instance of IServLetRequest
 */
public interface IServletResponse extends Closeable {

    /**
     * Set a response header
     * @param name
     * @param value
     * @return
     * @throws Exception
     */
    IServletResponse setHeader(CharSequence name, Object value) throws IOException;

    /**
     * Get the output writer for the response
     * @return
     * @throws Exception
     */
    PrintWriter writer();

    /**
     * Send the last http response if any and return the future
     * @return
     * @throws Exception
     */
    ChannelFuture future() throws IOException;

    /**
     * Set the status of the http response
     * @param status
     */
    void setStatus(HttpResponseStatus status);

    /**
     * Get the output stream for the response
     * @return
     */
    OutputStream outputStream();

    public static void setContentType(IServletResponse response, String type, String charset) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type + "; charset=" + charset);
    }

    public static void setContentType(IServletResponse response, String type) throws IOException {
        response.setHeader(HttpHeaderNames.CONTENT_TYPE, type);
    }
}
