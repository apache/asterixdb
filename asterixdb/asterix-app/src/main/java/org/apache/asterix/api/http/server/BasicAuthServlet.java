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

import static org.apache.asterix.api.http.server.ServletConstants.CREDENTIAL_MAP;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.mindrot.jbcrypt.BCrypt;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;

public abstract class BasicAuthServlet extends AbstractServlet {

    private static final Logger LOGGER = LogManager.getLogger();
    public static String BASIC_AUTH_METHOD_NAME = "Basic";
    private Base64.Decoder b64Decoder;
    Map<String, String> storedCredentials;

    protected BasicAuthServlet(ConcurrentMap<String, Object> ctx, String... paths) {
        super(ctx, paths);
        b64Decoder = Base64.getDecoder();
        storedCredentials = (Map<String, String>) ctx.get(CREDENTIAL_MAP);
    }

    @Override
    public void handle(IServletRequest request, IServletResponse response) {
        try {
            boolean authorized = authorize(request);
            if (!authorized) {
                response.setStatus(HttpResponseStatus.UNAUTHORIZED);
            } else {
                super.handle(request, response);
            }
        } catch (Exception e) {
            LOGGER.log(Level.WARN, "Unhandled exception", e);
            response.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        } catch (Throwable th) { //NOSONAR Just logging and then throwing again
            try {
                LOGGER.log(Level.WARN, "Unhandled throwable", th);
            } catch (Throwable loggingFailure) {// NOSONAR... swallow logging failure
            }
            throw th;
        }
    }

    private boolean authorize(IServletRequest request) {
        String authVal = request.getHeader(HttpHeaderNames.AUTHORIZATION);
        if (authVal == null) {
            LOGGER.debug("Request missing Authorization header");
            return false;
        }
        String[] authString = authVal.split(" ");
        if (!BASIC_AUTH_METHOD_NAME.equals(authString[0]) || authString.length != 2) {
            LOGGER.debug("Malformed Authorization header or unsupported Authentication method");
            return false;
        }
        String credentialEncoded = authString[1];
        String credential = new String(b64Decoder.decode(credentialEncoded));
        String[] providedCredentials = credential.split(":");
        if (providedCredentials.length != 2) {
            LOGGER.debug("Invalid Basic credential format");
            return false;
        }
        String providedUsername = providedCredentials[0];
        String storedPw = getStoredCredentials(request).get(providedUsername);
        if (storedPw == null) {
            LOGGER.debug("Invalid username");
            return false;
        }
        String givenPw = providedCredentials[1];
        if (BCrypt.checkpw(givenPw, storedPw)) {
            return true;
        } else {
            LOGGER.debug("Wrong password for user " + providedUsername);
            return false;
        }
    }

    protected Map<String, String> getStoredCredentials(IServletRequest request) {
        return storedCredentials;
    }

    public static String hashPassword(String password) {
        return BCrypt.hashpw(password, BCrypt.gensalt(12));
    }

    public static String createAuthHeader(String user, String password) {
        String auth = user + ":" + password;
        byte[] encodedAuth = Base64.getEncoder().encode(auth.getBytes(StandardCharsets.ISO_8859_1));
        return "Basic " + new String(encodedAuth);
    }
}
