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

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;

//Based in part on LoggingHandler from Netty
public class CLFLogger extends ChannelDuplexHandler {

    private static final Logger accessLogger = LogManager.getLogger();
    private static final Level ACCESS_LOG_LEVEL = Level.forName("ACCESS", 550);
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z").withZone(ZoneId.systemDefault());
    private StringBuilder logLineBuilder;

    private String clientIp;
    private Instant requestTime;
    private String reqLine;
    private int statusCode;
    private long respSize;
    private String userAgentRef;
    private boolean lastChunk = false;

    public CLFLogger() {
        this.logLineBuilder = new StringBuilder();
        respSize = 0;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest) {
            HttpRequest req = (HttpRequest) msg;
            clientIp = ((NioSocketChannel) ctx.channel()).remoteAddress().getAddress().toString().substring(1);
            requestTime = Instant.now();
            reqLine = req.method().toString() + " " + req.getUri() + " " + req.getProtocolVersion().toString();
            userAgentRef = headerValueOrDash("Referer", req) + " " + headerValueOrDash("User-Agent", req);
            lastChunk = false;
        }
        ctx.fireChannelRead(msg);
    }

    private String headerValueOrDash(String headerKey, HttpRequest req) {
        String value = req.headers().get(headerKey);
        if (value == null) {
            value = "-";
        } else {
            value = "\"" + value + "\"";
        }
        return value;

    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        if (msg instanceof DefaultHttpResponse) {
            HttpResponse resp = (DefaultHttpResponse) msg;
            statusCode = resp.status().code();
            if (msg instanceof DefaultFullHttpResponse) {
                lastChunk = true;
                respSize = resp.headers().getInt(HttpHeaderNames.CONTENT_LENGTH, 0);
            }
        } else if (msg instanceof DefaultHttpContent) {
            HttpContent content = (DefaultHttpContent) msg;

            respSize += content.content().readableBytes();
        } else if (msg instanceof LastHttpContent) {
            lastChunk = true;
        }

        ctx.write(msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        if (lastChunk) {
            printAndPrepare();
            lastChunk = false;
        }
        ctx.flush();
    }

    private void printAndPrepare() {
        if (!accessLogger.isEnabled(ACCESS_LOG_LEVEL)) {
            return;
        }
        logLineBuilder.append(clientIp);
        //identd value - not relevant here
        logLineBuilder.append(" - ");
        //no http auth or any auth either for that matter
        logLineBuilder.append(" - [");
        logLineBuilder.append(DATE_TIME_FORMATTER.format(requestTime));
        logLineBuilder.append("] \"");
        logLineBuilder.append(reqLine);
        logLineBuilder.append("\"");
        logLineBuilder.append(" ").append(statusCode);
        logLineBuilder.append(" ").append(respSize);
        logLineBuilder.append(" ").append(userAgentRef);
        accessLogger.log(ACCESS_LOG_LEVEL, logLineBuilder);
        respSize = 0;
        logLineBuilder.setLength(0);
    }
}
