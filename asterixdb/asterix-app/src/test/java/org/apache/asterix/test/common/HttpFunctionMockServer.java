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
package org.apache.asterix.test.common;

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_MOVED_TEMP;
import static java.net.HttpURLConnection.HTTP_MULT_CHOICE;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

/**
 * The HTTP server that the external HTTP functions of the tests call, at {@code http://127.0.0.1:19371}, and over TLS
 * at {@code https://127.0.0.1:19372} with the self-signed certificate of {@code security/http-function/server.p12},
 * issued to {@code IP:127.0.0.1}. Each route but {@code /token} and {@code /oauth/echo} answers a request the same way
 * every time, so a result does not depend on how many partitions call it or in which order. Those two share the tokens
 * issued, and each token issued to a client revokes its previous one, so a test that shows a token uses a client that
 * no other test uses, from a single query that calls it from one partition.
 * <ul>
 * <li>{@code /echo...}: a JSON object describing the request: its method, raw path, raw query, {@code X-} headers,
 * {@code Authorization} header, Content-Type without its multipart boundary, and either its JSON body or, for an
 * {@code application/x-www-form-urlencoded} or {@code multipart/form-data} body, its fields as {@code form}. A form
 * body that cannot be parsed is answered with a 400.</li>
 * <li>{@code /content/<kind>}: a body of one kind: {@code json}, {@code problem-json}, {@code gzip} (gzip-encoded
 * JSON), {@code text}, {@code empty-text} (text with an empty body), {@code latin1} (ISO-8859-1 text),
 * {@code xml}, {@code none} (no Content-Type) or {@code malformed} (invalid JSON)</li>
 * <li>{@code /status/<code>}: that status, with an empty body for a 2xx</li>
 * <li>{@code /redirect}: a 302 to {@code /echo}</li>
 * <li>{@code /sized/<length>}: a JSON string body of exactly that many bytes</li>
 * <li>{@code /slow}: a response that comes later than the timeout of any test</li>
 * <li>{@code /token}: an OAuth 2.0 token endpoint for the client credentials grant, authenticating a client by its id
 * and secret form-encoded into Basic credentials. Every client has the secret {@code CLIENT_SECRET}, which form
 * encoding changes, so a client that does not encode it is refused. A token is {@code <client id>.<n>}, the nth issued
 * to that client, followed by {@code .<scope>} form-encoded when a scope is requested, and expires in an hour; but the
 * tokens of client {@code short} expire in 30 seconds, the response to {@code malformed} has no token, and that to
 * {@code mac} is of type {@code mac}. A request without the client's credentials is answered with a 401, any other bad
 * request with a 400.</li>
 * <li>{@code /oauth/echo...}: as {@code /echo} if the request carries, as its bearer token, the token issued last to a
 * client, and a 401 otherwise. The expiry of a token is not checked.</li>
 * <li>{@code /throttled/<status>/<failures>/<key>}: that status for the first {@code failures} requests for
 * {@code key}, with the query parameter {@code after} as its {@code Retry-After} header if given, and then the number
 * of requests for {@code key} as {@code {"attempts": n}}. A test uses a key that no other test uses.</li>
 * </ul>
 */
public final class HttpFunctionMockServer {

    private static final String HOST = "127.0.0.1";
    private static final int PORT = 19371;
    private static final int TLS_PORT = 19372;
    private static final Path KEY_STORE = Path.of("src/test/resources/security/http-function/server.p12");
    private static final char[] KEY_STORE_PASSWORD = "http-function".toCharArray();
    private static final String KEY_STORE_TYPE = "PKCS12";
    private static final String TLS_PROTOCOL = "TLS";
    // partitions call the server concurrently, and each call to /slow holds a thread until it answers
    private static final int THREADS = 8;
    private static final long SLOW_RESPONSE_DELAY_SECONDS = 10;
    private static final int JSON_STRING_QUOTES_LENGTH = 2;
    private static final String HEADER_CONTENT_TYPE = "Content-Type";
    private static final String HEADER_CONTENT_ENCODING = "Content-Encoding";
    private static final String GZIP = "gzip";
    private static final String HEADER_LOCATION = "Location";
    private static final String HEADER_AUTHORIZATION = "Authorization";
    private static final String HEADER_CONTENT_DISPOSITION = "content-disposition";
    private static final String FORM_URLENCODED = "application/x-www-form-urlencoded";
    private static final String MULTIPART_FORM_DATA = "multipart/form-data";
    private static final String MEDIA_TYPE_PARAMETER_SEPARATOR = ";";
    private static final String BOUNDARY_PARAMETER = "boundary=";
    private static final String QUOTE = "\"";
    private static final String CRLF = "\r\n";
    private static final String MULTIPART_DELIMITER_PREFIX = CRLF + "--";
    private static final String MULTIPART_CLOSE_SUFFIX = "--";
    private static final String MULTIPART_HEADERS_END = CRLF + CRLF;
    private static final String HEADER_NAME_SEPARATOR = ":";
    private static final Pattern FORM_DATA_DISPOSITION = Pattern.compile("form-data;\\s*name=\"([^\"]*)\"");
    private static final String FORM_FIELD_SEPARATOR = "&";
    private static final String FORM_VALUE_SEPARATOR = "=";
    private static final String ECHOED_HEADER_PREFIX = "x-";
    private static final String APPLICATION_JSON = "application/json";
    private static final byte[] NO_BODY = new byte[0];
    private static final int NO_RESPONSE_BODY = -1;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String METHOD_POST = "POST";
    private static final String HEADER_WWW_AUTHENTICATE = "WWW-Authenticate";
    private static final String BASIC_SCHEME = "Basic";
    private static final String BEARER_SCHEME = "Bearer";
    private static final String SCHEME_SEPARATOR = " ";
    private static final String BASIC_CREDENTIALS_SEPARATOR = ":";
    private static final String CLIENT_SECRET = "se+cr:et";
    private static final String GRANT_TYPE = "grant_type";
    private static final String CLIENT_CREDENTIALS_GRANT = "client_credentials";
    private static final String SCOPE = "scope";
    private static final String ACCESS_TOKEN = "access_token";
    private static final String TOKEN_TYPE = "token_type";
    private static final String MAC_TOKEN_TYPE = "mac";
    private static final String EXPIRES_IN = "expires_in";
    private static final long TOKEN_LIFETIME_SECONDS = TimeUnit.HOURS.toSeconds(1);
    private static final long SHORT_TOKEN_LIFETIME_SECONDS = 30;
    private static final String SHORT_LIVED_TOKEN_CLIENT = "short";
    private static final String NO_TOKEN_CLIENT = "malformed";
    private static final String MAC_TOKEN_CLIENT = "mac";
    private static final String TOKEN_PART_SEPARATOR = ".";
    private static final String OAUTH_ERROR = "error";
    private static final String INVALID_CLIENT = "invalid_client";
    private static final String INVALID_REQUEST = "invalid_request";
    private static final String UNSUPPORTED_GRANT_TYPE = "unsupported_grant_type";
    private static final String INVALID_TOKEN = "invalid_token";
    private static final String HEADER_RETRY_AFTER = "Retry-After";
    private static final String RETRY_AFTER_PARAMETER = "after";

    private final List<HttpServer> servers;
    private final ExecutorService executor;
    // by client id; guarded by this
    private final Map<String, Integer> issuedTokenCounts = new HashMap<>();
    private final Map<String, String> currentTokens = new HashMap<>();
    // by key of /throttled; guarded by this
    private final Map<String, Integer> throttledRequestCounts = new HashMap<>();

    private HttpFunctionMockServer() throws IOException, GeneralSecurityException {
        HttpsServer tlsServer = HttpsServer.create(new InetSocketAddress(HOST, TLS_PORT), 0);
        tlsServer.setHttpsConfigurator(new HttpsConfigurator(createSslContext()));
        servers = List.of(HttpServer.create(new InetSocketAddress(HOST, PORT), 0), tlsServer);
        executor = Executors.newFixedThreadPool(THREADS);
        servers.forEach(server -> server.setExecutor(executor));
        route("/echo", HttpFunctionMockServer::echo);
        route("/content/", HttpFunctionMockServer::content);
        route("/status/", HttpFunctionMockServer::status);
        route("/redirect", (exchange, body) -> {
            exchange.getResponseHeaders().set(HEADER_LOCATION, "/echo");
            respond(exchange, HTTP_MOVED_TEMP, null, NO_BODY);
        });
        route("/sized/", HttpFunctionMockServer::sized);
        route("/slow", HttpFunctionMockServer::slow);
        route("/token", this::token);
        route("/oauth/echo", this::oauthEcho);
        route("/throttled/", this::throttled);
    }

    public static HttpFunctionMockServer start() throws IOException, GeneralSecurityException {
        HttpFunctionMockServer mockServer = new HttpFunctionMockServer();
        mockServer.servers.forEach(HttpServer::start);
        return mockServer;
    }

    public void stop() {
        servers.forEach(server -> server.stop(0));
        // interrupts the handlers still answering /slow
        executor.shutdownNow();
    }

    private static SSLContext createSslContext() throws IOException, GeneralSecurityException {
        KeyStore keyStore = KeyStore.getInstance(KEY_STORE_TYPE);
        try (InputStream in = Files.newInputStream(KEY_STORE)) {
            keyStore.load(in, KEY_STORE_PASSWORD);
        }
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keyStore, KEY_STORE_PASSWORD);
        SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
        sslContext.init(keyManagers.getKeyManagers(), null, null);
        return sslContext;
    }

    private void route(String path, Handler handler) {
        servers.forEach(server -> server.createContext(path, exchange -> {
            try (exchange) {
                // a client may not read the response before it has sent the whole request
                byte[] body = exchange.getRequestBody().readAllBytes();
                handler.handle(exchange, body);
            }
        }));
    }

    private static void echo(HttpExchange exchange, byte[] body) throws IOException {
        URI uri = exchange.getRequestURI();
        ObjectNode echo = OBJECT_MAPPER.createObjectNode();
        echo.put("method", exchange.getRequestMethod());
        echo.put("path", uri.getRawPath());
        if (uri.getRawQuery() != null) {
            echo.put("query", uri.getRawQuery());
        }
        Map<String, String> headers = new TreeMap<>();
        exchange.getRequestHeaders().forEach((name, values) -> {
            String lowerCaseName = name.toLowerCase(Locale.ROOT);
            if (lowerCaseName.startsWith(ECHOED_HEADER_PREFIX)) {
                headers.put(lowerCaseName, String.join(",", values));
            }
        });
        if (!headers.isEmpty()) {
            echo.set("headers", OBJECT_MAPPER.valueToTree(headers));
        }
        List<String> authorization = exchange.getRequestHeaders().get(HEADER_AUTHORIZATION);
        if (authorization != null) {
            // joined rather than the first, so that a duplicate Authorization header shows
            echo.put("authorization", String.join(",", authorization));
        }
        if (body.length > 0) {
            String contentType = exchange.getRequestHeaders().getFirst(HEADER_CONTENT_TYPE);
            List<String> mediaType = Arrays.stream(contentType.split(MEDIA_TYPE_PARAMETER_SEPARATOR)).map(String::trim)
                    .collect(Collectors.toList());
            String mimeType = mediaType.get(0).toLowerCase(Locale.ROOT);
            // the boundary is random, so a result that showed it would differ on every run
            echo.put("contentType", mediaType.stream().filter(parameter -> !parameter.startsWith(BOUNDARY_PARAMETER))
                    .collect(Collectors.joining(MEDIA_TYPE_PARAMETER_SEPARATOR + " ")));
            try {
                switch (mimeType) {
                    case FORM_URLENCODED -> echo.set("form", parseUrlEncoded(body));
                    case MULTIPART_FORM_DATA -> echo.set("form", parseMultipart(body, boundary(mediaType)));
                    default -> echo.set("body", OBJECT_MAPPER.readTree(body));
                }
            } catch (IllegalArgumentException e) {
                respond(exchange, HTTP_BAD_REQUEST, null, NO_BODY);
                return;
            }
        }
        respond(exchange, HTTP_OK, APPLICATION_JSON, OBJECT_MAPPER.writeValueAsBytes(echo));
    }

    private static ObjectNode parseUrlEncoded(byte[] body) {
        ObjectNode form = OBJECT_MAPPER.createObjectNode();
        for (String field : new String(body, StandardCharsets.US_ASCII).split(FORM_FIELD_SEPARATOR)) {
            int separator = field.indexOf(FORM_VALUE_SEPARATOR);
            if (separator < 0) {
                throw new IllegalArgumentException("form field without a value: " + field);
            }
            form.put(URLDecoder.decode(field.substring(0, separator), StandardCharsets.UTF_8),
                    URLDecoder.decode(field.substring(separator + 1), StandardCharsets.UTF_8));
        }
        return form;
    }

    private static String boundary(List<String> mediaType) {
        String boundary = mediaType.stream().filter(parameter -> parameter.startsWith(BOUNDARY_PARAMETER))
                .map(parameter -> parameter.substring(BOUNDARY_PARAMETER.length())).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("multipart body without a boundary"));
        return boundary.startsWith(QUOTE) && boundary.endsWith(QUOTE) && boundary.length() > QUOTE.length()
                ? boundary.substring(QUOTE.length(), boundary.length() - QUOTE.length()) : boundary;
    }

    private static ObjectNode parseMultipart(byte[] body, String boundary) {
        // a leading CRLF lets the first delimiter be found like every other one
        String text = CRLF + new String(body, StandardCharsets.UTF_8);
        String[] parts = text.split(Pattern.quote(MULTIPART_DELIMITER_PREFIX + boundary), -1);
        ObjectNode form = OBJECT_MAPPER.createObjectNode();
        // parts[0] is the preamble
        for (int i = 1; i < parts.length; i++) {
            String part = parts[i];
            if (part.startsWith(MULTIPART_CLOSE_SUFFIX)) {
                return form;
            }
            int headersEnd = part.indexOf(MULTIPART_HEADERS_END);
            if (!part.startsWith(CRLF) || headersEnd < CRLF.length()) {
                throw new IllegalArgumentException("malformed multipart part: " + part);
            }
            String name = null;
            for (String header : part.substring(CRLF.length(), headersEnd).split(CRLF)) {
                int separator = header.indexOf(HEADER_NAME_SEPARATOR);
                if (separator > 0
                        && header.substring(0, separator).trim().equalsIgnoreCase(HEADER_CONTENT_DISPOSITION)) {
                    Matcher disposition = FORM_DATA_DISPOSITION.matcher(header.substring(separator + 1).trim());
                    if (disposition.lookingAt()) {
                        name = disposition.group(1);
                    }
                }
            }
            if (name == null) {
                throw new IllegalArgumentException("multipart part without a form-data name: " + part);
            }
            form.put(name, part.substring(headersEnd + MULTIPART_HEADERS_END.length()));
        }
        throw new IllegalArgumentException("multipart body without a closing delimiter");
    }

    private static void content(HttpExchange exchange, byte[] body) throws IOException {
        switch (lastPathSegment(exchange)) {
            case "json" -> respond(exchange, HTTP_OK, "application/json; charset=utf-8", "{\"kind\":\"json\"}");
            case "problem-json" -> respond(exchange, HTTP_OK, "application/problem+json", "{\"kind\":\"problem\"}");
            case "gzip" -> {
                exchange.getResponseHeaders().set(HEADER_CONTENT_ENCODING, GZIP);
                respond(exchange, HTTP_OK, APPLICATION_JSON, gzip("{\"kind\":\"gzip\"}"));
            }
            case "text" -> respond(exchange, HTTP_OK, "text/plain; charset=utf-8", "hello");
            case "empty-text" -> respond(exchange, HTTP_OK, "text/plain; charset=utf-8", NO_BODY);
            case "latin1" -> respond(exchange, HTTP_OK, "text/plain; charset=iso-8859-1",
                    "café".getBytes(StandardCharsets.ISO_8859_1));
            case "xml" -> respond(exchange, HTTP_OK, "application/xml", "<kind>xml</kind>");
            case "none" -> respond(exchange, HTTP_OK, null, "{\"kind\":\"none\"}");
            case "malformed" -> respond(exchange, HTTP_OK, APPLICATION_JSON, "{\"kind\":");
            default -> respond(exchange, HTTP_NOT_FOUND, null, NO_BODY);
        }
    }

    private static void status(HttpExchange exchange, byte[] body) throws IOException {
        int status = Integer.parseInt(lastPathSegment(exchange));
        if (status == HTTP_NO_CONTENT) {
            respond(exchange, status, null, NO_BODY);
        } else if (status >= HTTP_OK && status < HTTP_MULT_CHOICE) {
            respond(exchange, status, APPLICATION_JSON, NO_BODY);
        } else {
            respond(exchange, status, APPLICATION_JSON, "{\"status\":" + status + "}");
        }
    }

    private static void sized(HttpExchange exchange, byte[] body) throws IOException {
        int length = Integer.parseInt(lastPathSegment(exchange));
        respond(exchange, HTTP_OK, APPLICATION_JSON,
                OBJECT_MAPPER.writeValueAsBytes("x".repeat(length - JSON_STRING_QUOTES_LENGTH)));
    }

    private static void slow(HttpExchange exchange, byte[] body) throws IOException {
        try {
            TimeUnit.SECONDS.sleep(SLOW_RESPONSE_DELAY_SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        respond(exchange, HTTP_OK, APPLICATION_JSON, "{}");
    }

    private void token(HttpExchange exchange, byte[] body) throws IOException {
        ClientCredentials client = clientCredentials(exchange);
        if (client == null || !CLIENT_SECRET.equals(client.secret())) {
            challenge(exchange, BASIC_SCHEME, INVALID_CLIENT);
            return;
        }
        String contentType = exchange.getRequestHeaders().getFirst(HEADER_CONTENT_TYPE);
        if (!METHOD_POST.equals(exchange.getRequestMethod()) || contentType == null
                || !contentType.split(MEDIA_TYPE_PARAMETER_SEPARATOR)[0].trim().equalsIgnoreCase(FORM_URLENCODED)) {
            respondOAuthError(exchange, HTTP_BAD_REQUEST, INVALID_REQUEST);
            return;
        }
        ObjectNode form;
        try {
            form = parseUrlEncoded(body);
        } catch (IllegalArgumentException e) {
            respondOAuthError(exchange, HTTP_BAD_REQUEST, INVALID_REQUEST);
            return;
        }
        if (!CLIENT_CREDENTIALS_GRANT.equals(form.path(GRANT_TYPE).textValue())) {
            respondOAuthError(exchange, HTTP_BAD_REQUEST, UNSUPPORTED_GRANT_TYPE);
            return;
        }
        ObjectNode response = OBJECT_MAPPER.createObjectNode();
        if (!NO_TOKEN_CLIENT.equals(client.id())) {
            response.put(ACCESS_TOKEN, issueToken(client.id(), form.path(SCOPE).textValue()));
        }
        response.put(TOKEN_TYPE, MAC_TOKEN_CLIENT.equals(client.id()) ? MAC_TOKEN_TYPE : BEARER_SCHEME);
        response.put(EXPIRES_IN,
                SHORT_LIVED_TOKEN_CLIENT.equals(client.id()) ? SHORT_TOKEN_LIFETIME_SECONDS : TOKEN_LIFETIME_SECONDS);
        respond(exchange, HTTP_OK, APPLICATION_JSON, OBJECT_MAPPER.writeValueAsBytes(response));
    }

    private synchronized String issueToken(String clientId, String scope) {
        String token = clientId + TOKEN_PART_SEPARATOR + issuedTokenCounts.merge(clientId, 1, Integer::sum);
        if (scope != null) {
            token += TOKEN_PART_SEPARATOR + URLEncoder.encode(scope, StandardCharsets.UTF_8);
        }
        currentTokens.put(clientId, token);
        return token;
    }

    private synchronized boolean isCurrentToken(String token) {
        return currentTokens.containsValue(token);
    }

    private void oauthEcho(HttpExchange exchange, byte[] body) throws IOException {
        String token = credentials(exchange, BEARER_SCHEME);
        if (token == null || !isCurrentToken(token)) {
            challenge(exchange, BEARER_SCHEME, INVALID_TOKEN);
            return;
        }
        echo(exchange, body);
    }

    private void throttled(HttpExchange exchange, byte[] body) throws IOException {
        URI uri = exchange.getRequestURI();
        String[] segments = uri.getPath().split("/");
        int status = Integer.parseInt(segments[2]);
        int failures = Integer.parseInt(segments[3]);
        int attempts;
        synchronized (this) {
            attempts = throttledRequestCounts.merge(segments[4], 1, Integer::sum);
        }
        if (attempts > failures) {
            respond(exchange, HTTP_OK, APPLICATION_JSON, "{\"attempts\":" + attempts + "}");
            return;
        }
        if (uri.getRawQuery() != null) {
            String retryAfter = parseUrlEncoded(uri.getRawQuery().getBytes(StandardCharsets.US_ASCII))
                    .path(RETRY_AFTER_PARAMETER).textValue();
            if (retryAfter != null) {
                exchange.getResponseHeaders().set(HEADER_RETRY_AFTER, retryAfter);
            }
        }
        respond(exchange, status, APPLICATION_JSON, "{\"status\":" + status + "}");
    }

    private static ClientCredentials clientCredentials(HttpExchange exchange) {
        String credentials = credentials(exchange, BASIC_SCHEME);
        if (credentials == null) {
            return null;
        }
        try {
            String decoded = new String(Base64.getDecoder().decode(credentials), StandardCharsets.UTF_8);
            int separator = decoded.indexOf(BASIC_CREDENTIALS_SEPARATOR);
            return separator < 0 ? null
                    : new ClientCredentials(URLDecoder.decode(decoded.substring(0, separator), StandardCharsets.UTF_8),
                            URLDecoder.decode(decoded.substring(separator + 1), StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            // not Base64, or not form-encoded
            return null;
        }
    }

    /**
     * @return the credentials of the request's Authorization header if it uses the given scheme, else {@code null}
     */
    private static String credentials(HttpExchange exchange, String scheme) {
        String authorization = exchange.getRequestHeaders().getFirst(HEADER_AUTHORIZATION);
        String prefix = scheme + SCHEME_SEPARATOR;
        // an authentication scheme is case-insensitive
        return authorization != null && authorization.regionMatches(true, 0, prefix, 0, prefix.length())
                ? authorization.substring(prefix.length()) : null;
    }

    private static void challenge(HttpExchange exchange, String scheme, String error) throws IOException {
        exchange.getResponseHeaders().set(HEADER_WWW_AUTHENTICATE, scheme);
        respondOAuthError(exchange, HTTP_UNAUTHORIZED, error);
    }

    private static void respondOAuthError(HttpExchange exchange, int status, String error) throws IOException {
        respond(exchange, status, APPLICATION_JSON,
                OBJECT_MAPPER.writeValueAsBytes(OBJECT_MAPPER.createObjectNode().put(OAUTH_ERROR, error)));
    }

    private static String lastPathSegment(HttpExchange exchange) {
        String path = exchange.getRequestURI().getPath();
        return path.substring(path.lastIndexOf('/') + 1);
    }

    private static byte[] gzip(String body) throws IOException {
        ByteArrayOutputStream compressed = new ByteArrayOutputStream();
        try (GZIPOutputStream out = new GZIPOutputStream(compressed)) {
            out.write(body.getBytes(StandardCharsets.UTF_8));
        }
        return compressed.toByteArray();
    }

    private static void respond(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        respond(exchange, status, contentType, body.getBytes(StandardCharsets.UTF_8));
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body) throws IOException {
        if (contentType != null) {
            exchange.getResponseHeaders().set(HEADER_CONTENT_TYPE, contentType);
        }
        if (body.length == 0) {
            exchange.sendResponseHeaders(status, NO_RESPONSE_BODY);
        } else {
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
        }
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange, byte[] body) throws IOException;
    }

    private record ClientCredentials(String id, String secret) {
    }
}
