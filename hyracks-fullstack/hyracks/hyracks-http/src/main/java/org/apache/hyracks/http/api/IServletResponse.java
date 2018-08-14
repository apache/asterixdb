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
package org.apache.hyracks.http.api;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;

import io.netty.channel.ChannelFuture;
import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * A response to an instance of IServletRequest
 */
public interface IServletResponse extends Closeable {

    /**
     * Set a response header
     *
     * @param name
     *            the name of the header
     * @param value
     *            the value of the header
     * @return
     *         the servlet response with the header set
     * @throws IOException
     */
    IServletResponse setHeader(CharSequence name, Object value) throws IOException;

    /**
     * Set the status of the http response
     *
     * @param status
     */
    void setStatus(HttpResponseStatus status);

    /**
     * Get the output writer for the response which writes to the response output stream
     *
     * @return the response writer
     */
    PrintWriter writer();

    /**
     * Get the output stream for the response
     *
     * @return the response output stream
     */
    OutputStream outputStream();

    /**
     * Get last content future
     * Must only be called after the servlet response has been closed
     * Used to listen to events about the last content sent through the network
     * For example, to close the connection after the event has been completed
     * lastContentFuture().addListener(ChannelFutureListener.CLOSE);
     *
     * @return
     * @throws IOException
     */
    ChannelFuture lastContentFuture() throws IOException;

    /**
     * Notifies the response that the channel has become writable. Used for flow control
     */
    void notifyChannelWritable();

    /**
     * Notifies the response that the channel has become inactive.
     */
    void notifyChannelInactive();

    /**
     * Called on a created request that is cancelled before it is started
     */
    void cancel();
}
