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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.api.common.AsterixHyracksIntegrationUtil;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * A request whose job has finished has nothing left for a cancel to abort, so a cancel arriving while its result is
 * being sent would interrupt the thread writing that result - mid-write, in a response whose header is already on the
 * wire. Such a request must leave the cancellable set, so the cancel is refused and the response is delivered whole.
 */
public class CancelDuringResultStreamingTest {

    private static final AsterixHyracksIntegrationUtil INTEGRATION_UTIL = new AsterixHyracksIntegrationUtil();
    private static final int API_PORT = 19002;
    /**
     * A result far larger than any socket buffer can hold, so the writer is certainly still sending it when the
     * cancel arrives. A body the kernel can absorb whole lets the request finish and be untracked, and the cancel
     * then answers 404 rather than the 403 of a request that is there and refuses to be cancelled.
     */
    private static final int ROWS = 10000;
    private static final int PAD = 4000;
    /** How much of the body's end to keep, enough to carry the status and metrics. */
    private static final int TAIL = 8192;

    @BeforeClass
    public static void setUp() throws Exception {
        INTEGRATION_UTIL.init(true, AsterixHyracksIntegrationUtil.DEFAULT_CONF_FILE);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        INTEGRATION_UTIL.deinit(true);
    }

    @Test
    public void cancelIsRefusedWhileTheResultIsBeingSent() throws Exception {
        String clientContextId = UUID.randomUUID().toString();
        try (Socket socket = new Socket("localhost", API_PORT)) {
            socket.setSoTimeout((int) TimeUnit.MINUTES.toMillis(2));
            post(socket, clientContextId);
            InputStream body = new BufferedInputStream(socket.getInputStream(), 1 << 16);

            // the header is written with the first chunk of the result, so reading it means the job has finished
            // and the request is now sending rows; the body is left unread so that it still is when we cancel
            String head = readHead(body);
            Assert.assertTrue(head, head.startsWith("HTTP/1.1 200 OK"));

            Assert.assertEquals(
                    "a request that is only sending its result must refuse a cancel; 404 would mean it"
                            + " had finished already, the body having been small enough for the channel to take whole",
                    403, cancel(clientContextId));

            // and the response the client was half-way through must arrive whole, terminating chunk and all
            String tail = readChunkedBodyTail(body);
            Assert.assertTrue("the response did not report a success: " + tail, tail.contains("\"success\""));
        }
    }

    private static void post(Socket socket, String clientContextId) throws IOException {
        String statement = "SELECT r AS id, repeat(\"x\", " + PAD + ") AS pad FROM range(1, " + ROWS + ") r;";
        String form = "statement=" + URLEncoder.encode(statement, StandardCharsets.UTF_8) + "&client_context_id="
                + URLEncoder.encode(clientContextId, StandardCharsets.UTF_8);
        OutputStream out = socket.getOutputStream();
        out.write(("POST " + org.apache.asterix.common.utils.Servlets.QUERY_SERVICE + " HTTP/1.1\r\n"
                + "Host: localhost:" + API_PORT + "\r\n" + "Content-Type: application/x-www-form-urlencoded\r\n"
                + "Connection: close\r\n" + "Content-Length: " + form.getBytes(StandardCharsets.UTF_8).length
                + "\r\n\r\n" + form).getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private static int cancel(String clientContextId) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpDelete delete = new HttpDelete(
                    "http://localhost:" + API_PORT + "/admin/requests/running?client_context_id=" + clientContextId);
            try (CloseableHttpResponse response = httpClient.execute(delete)) {
                return response.getStatusLine().getStatusCode();
            }
        }
    }

    /** Reads the status line and headers, leaving the body unread. */
    private static String readHead(InputStream in) throws IOException {
        StringBuilder head = new StringBuilder();
        for (String line = readLine(in); !line.isEmpty(); line = readLine(in)) {
            head.append(line).append('\n');
        }
        return head.toString();
    }

    /**
     * Reads a chunked body to its terminating chunk, failing if the stream ends before one arrives, and returns
     * its last few KB - where the status and metrics of the response are - rather than holding all of it.
     */
    private static String readChunkedBodyTail(InputStream in) throws IOException {
        StringBuilder tail = new StringBuilder();
        long total = 0;
        while (true) {
            int size = Integer.parseInt(readLine(in).split(";")[0].trim(), 16);
            if (size == 0) {
                return tail.toString();
            }
            byte[] chunk = new byte[size];
            for (int read = 0; read < size;) {
                int n = in.read(chunk, read, size - read);
                if (n < 0) {
                    throw new IOException("the response was aborted after " + total + " bytes of body");
                }
                read += n;
            }
            total += size;
            tail.append(new String(chunk, StandardCharsets.UTF_8));
            if (tail.length() > TAIL) {
                tail.delete(0, tail.length() - TAIL);
            }
            readLine(in);
        }
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        for (int c = in.read(); c != '\n'; c = in.read()) {
            if (c < 0) {
                throw new IOException("the response ended mid-line: " + line.toString(StandardCharsets.UTF_8));
            }
            if (c != '\r') {
                line.write(c);
            }
        }
        return line.toString(StandardCharsets.UTF_8);
    }
}
