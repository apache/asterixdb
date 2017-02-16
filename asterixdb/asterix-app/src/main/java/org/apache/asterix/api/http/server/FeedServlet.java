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

import java.awt.image.BufferedImage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.imageio.ImageIO;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;

import io.netty.handler.codec.http.HttpResponseStatus;

public class FeedServlet extends AbstractServlet {
    private static final Logger LOGGER = Logger.getLogger(FeedServlet.class.getName());

    public FeedServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        try {
            response.setStatus(HttpResponseStatus.OK);
            String resourcePath;
            String requestURI = request.getHttpRequest().uri();

            if ("/".equals(requestURI)) {
                HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML);
                resourcePath = "/feed/home.html";
            } else {
                resourcePath = requestURI;
            }

            InputStream is = FeedServlet.class.getResourceAsStream(resourcePath);
            if (is == null) {
                response.setStatus(HttpResponseStatus.NOT_FOUND);
                return;
            }

            // Special handler for font files and .png resources
            if (resourcePath.endsWith(".png")) {

                BufferedImage img = ImageIO.read(is);
                OutputStream outputStream = response.outputStream();
                String formatName = "png";
                HttpUtil.setContentType(response, HttpUtil.ContentType.IMG_PNG);
                ImageIO.write(img, formatName, outputStream);
                return;
            }

            HttpUtil.setContentType(response, HttpUtil.ContentType.TEXT_HTML, HttpUtil.Encoding.UTF8);
            InputStreamReader isr = new InputStreamReader(is);
            StringBuilder sb = new StringBuilder();
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();

            while (line != null) {
                sb.append(line + "\n");
                line = br.readLine();
            }

            PrintWriter out = response.writer();
            out.println(sb.toString());
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Failure handling request", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
            return;
        }
    }
}
