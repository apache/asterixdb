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

package org.apache.asterix.jdbc.core;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpOptions;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.exc.InvalidDefinitionException;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class ADBProtocol {

    private static final String QUERY_ENDPOINT_PATH = "/query/service";
    private static final String QUERY_RESULT_ENDPOINT_PATH = "/query/service/result";

    private static final String STATEMENT = "statement";
    private static final String ARGS = "args";
    private static final String MODE = "mode";
    private static final String READ_ONLY = "readonly";
    private static final String DATAVERSE = "dataverse";
    private static final String TIMEOUT = "timeout";
    private static final String SIGNATURE = "signature";
    private static final String COMPILE_ONLY = "compile-only";
    private static final String CLIENT_TYPE = "client-type";
    private static final String PLAN_FORMAT = "plan-format";
    private static final String MAX_WARNINGS = "max-warnings";

    private static final String MODE_DEFERRED = "deferred";
    private static final String CLIENT_TYPE_JDBC = "jdbc";
    private static final String RESULTS = "results";
    private static final String FORMAT_LOSSLESS_ADM = "lossless-adm";
    private static final String PLAN_FORMAT_STRING = "string";

    private static final String OPTIONAL_TYPE_SUFFIX = "?";
    static final char TEXT_DELIMITER = ':';
    static final String EXPLAIN_ONLY_RESULT_COLUMN_NAME = "$1";
    static final String DEFAULT_DATAVERSE = "Default";

    final ADBDriverContext driverContext;
    final HttpClientConnectionManager httpConnectionManager;
    final HttpClientContext httpClientContext;
    final CloseableHttpClient httpClient;
    final URI queryEndpoint;
    final URI queryResultEndpoint;
    final String user;
    final int maxWarnings;

    protected ADBProtocol(String host, int port, Map<ADBDriverProperty, Object> params, ADBDriverContext driverContext)
            throws SQLException {
        URI queryEndpoint = createEndpointUri(host, port, QUERY_ENDPOINT_PATH, driverContext.errorReporter);
        URI queryResultEndpoint =
                createEndpointUri(host, port, QUERY_RESULT_ENDPOINT_PATH, driverContext.errorReporter);
        PoolingHttpClientConnectionManager httpConnectionManager = new PoolingHttpClientConnectionManager();
        int maxConnections = Math.max(16, Runtime.getRuntime().availableProcessors());
        httpConnectionManager.setDefaultMaxPerRoute(maxConnections);
        httpConnectionManager.setMaxTotal(maxConnections);
        SocketConfig.Builder socketConfigBuilder = null;
        Number socketTimeoutMillis = (Number) params.get(ADBDriverProperty.Common.SOCKET_TIMEOUT);
        if (socketTimeoutMillis != null) {
            socketConfigBuilder = SocketConfig.custom();
            socketConfigBuilder.setSoTimeout(socketTimeoutMillis.intValue());
        }
        if (socketConfigBuilder != null) {
            httpConnectionManager.setDefaultSocketConfig(socketConfigBuilder.build());
        }
        RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
        Number connectTimeoutMillis = (Number) params.get(ADBDriverProperty.Common.CONNECT_TIMEOUT);
        if (connectTimeoutMillis != null) {
            requestConfigBuilder.setConnectionRequestTimeout(connectTimeoutMillis.intValue());
            requestConfigBuilder.setConnectTimeout(connectTimeoutMillis.intValue());
        }
        if (socketTimeoutMillis != null) {
            requestConfigBuilder.setSocketTimeout(socketTimeoutMillis.intValue());
        }
        RequestConfig requestConfig = requestConfigBuilder.build();

        HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
        httpClientBuilder.setConnectionManager(httpConnectionManager);
        httpClientBuilder.setConnectionManagerShared(true);
        httpClientBuilder.setDefaultRequestConfig(requestConfig);
        String user = (String) params.get(ADBDriverProperty.Common.USER);
        if (user != null) {
            String password = (String) params.get(ADBDriverProperty.Common.PASSWORD);
            httpClientBuilder.setDefaultCredentialsProvider(createCredentialsProvider(user, password));
        }

        Number maxWarnings = ((Number) params.getOrDefault(ADBDriverProperty.Common.MAX_WARNINGS,
                ADBDriverProperty.Common.MAX_WARNINGS.getDefaultValue()));

        this.user = user;
        this.queryEndpoint = queryEndpoint;
        this.queryResultEndpoint = queryResultEndpoint;
        this.httpConnectionManager = httpConnectionManager;
        this.httpClient = httpClientBuilder.build();
        this.httpClientContext = createHttpClientContext(queryEndpoint);
        this.driverContext = Objects.requireNonNull(driverContext);
        this.maxWarnings = Math.max(maxWarnings.intValue(), 0);
    }

    private static URI createEndpointUri(String host, int port, String path, ADBErrorReporter errorReporter)
            throws SQLException {
        try {
            return new URI("http", null, host, port, path, null, null);
        } catch (URISyntaxException e) {
            throw errorReporter.errorParameterValueNotSupported("endpoint " + host + ":" + port);
        }
    }

    private static CredentialsProvider createCredentialsProvider(String user, String password) {
        CredentialsProvider cp = new BasicCredentialsProvider();
        cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(user, password));
        return cp;
    }

    void close() throws SQLException {
        try {
            httpClient.close();
        } catch (IOException e) {
            throw getErrorReporter().errorClosingResource(e);
        } finally {
            httpConnectionManager.shutdown();
        }
    }

    String connect() throws SQLException {
        String databaseVersion = pingImpl(-1, true); // TODO:review timeout
        if (getLogger().isLoggable(Level.FINE)) {
            getLogger().log(Level.FINE, String.format("connected to '%s' at %s", databaseVersion, queryEndpoint));
        }
        return databaseVersion;
    }

    boolean ping(int timeoutSeconds) {
        try {
            pingImpl(timeoutSeconds, false);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private String pingImpl(int timeoutSeconds, boolean fetchDatabaseVersion) throws SQLException {
        //TODO: support timeoutSeconds: -1 = use default, 0 = indefinite ?
        HttpOptions httpOptions = new HttpOptions(queryEndpoint);
        try (CloseableHttpResponse response = httpClient.execute(httpOptions, httpClientContext)) {
            int statusCode = response.getStatusLine().getStatusCode();
            switch (statusCode) {
                case HttpStatus.SC_OK:
                    String databaseVersion = null;
                    if (fetchDatabaseVersion) {
                        Header serverHeader = response.getFirstHeader(HttpHeaders.SERVER);
                        if (serverHeader != null) {
                            databaseVersion = serverHeader.getValue();
                        }
                    }
                    return databaseVersion;
                case HttpStatus.SC_UNAUTHORIZED:
                case HttpStatus.SC_FORBIDDEN:
                    throw getErrorReporter().errorAuth();
                default:
                    throw getErrorReporter().errorInConnection(String.valueOf(response.getStatusLine()));
            }
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    QueryServiceResponse submitStatement(String sql, List<?> args, boolean forceReadOnly, boolean compileOnly,
            int timeoutSeconds, String catalog, String schema) throws SQLException {
        HttpPost httpPost = new HttpPost(queryEndpoint);
        httpPost.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON
                .withParameters(new BasicNameValuePair(FORMAT_LOSSLESS_ADM, Boolean.TRUE.toString())).toString());

        ByteArrayOutputStreamImpl baos = new ByteArrayOutputStreamImpl(512);
        try {
            JsonGenerator jsonGen = driverContext.genericObjectWriter.createGenerator(baos, JsonEncoding.UTF8);
            jsonGen.writeStartObject();
            jsonGen.writeStringField(CLIENT_TYPE, CLIENT_TYPE_JDBC);
            jsonGen.writeStringField(MODE, MODE_DEFERRED);
            jsonGen.writeStringField(STATEMENT, sql);
            jsonGen.writeBooleanField(SIGNATURE, true);
            jsonGen.writeStringField(PLAN_FORMAT, PLAN_FORMAT_STRING);
            jsonGen.writeNumberField(MAX_WARNINGS, maxWarnings);
            if (compileOnly) {
                jsonGen.writeBooleanField(COMPILE_ONLY, true);
            }
            if (forceReadOnly) {
                jsonGen.writeBooleanField(READ_ONLY, true);
            }
            if (timeoutSeconds > 0) {
                jsonGen.writeStringField(TIMEOUT, timeoutSeconds + "s");
            }
            if (catalog != null) {
                jsonGen.writeStringField(DATAVERSE, schema != null ? catalog + "/" + schema : catalog);
            }
            if (args != null && !args.isEmpty()) {
                jsonGen.writeFieldName(ARGS);
                driverContext.admFormatObjectWriter.writeValue(jsonGen, args);
            }
            jsonGen.writeEndObject();
            jsonGen.flush();
        } catch (InvalidDefinitionException e) {
            throw getErrorReporter().errorUnexpectedType(e.getType().getRawClass());
        } catch (IOException e) {
            throw getErrorReporter().errorInRequestGeneration(e);
        }

        System.err.printf("<ADB_DRIVER_SQL>%n%s%n</ADB_DRIVER_SQL>%n", sql);

        if (getLogger().isLoggable(Level.FINE)) {
            getLogger().log(Level.FINE, String.format("%s { %s } with args { %s }", compileOnly ? "compile" : "execute",
                    sql, args != null ? args : ""));
        }

        httpPost.setEntity(new EntityTemplateImpl(baos, ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse httpResponse = httpClient.execute(httpPost, httpClientContext)) {
            return handlePostQueryResponse(httpResponse);
        } catch (JsonProcessingException e) {
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private QueryServiceResponse handlePostQueryResponse(CloseableHttpResponse httpResponse)
            throws SQLException, IOException {
        int httpStatus = httpResponse.getStatusLine().getStatusCode();
        switch (httpStatus) {
            case HttpStatus.SC_OK:
            case HttpStatus.SC_BAD_REQUEST:
            case HttpStatus.SC_INTERNAL_SERVER_ERROR:
            case HttpStatus.SC_SERVICE_UNAVAILABLE:
                break;
            case HttpStatus.SC_UNAUTHORIZED:
            case HttpStatus.SC_FORBIDDEN:
                throw getErrorReporter().errorAuth();
            default:
                throw getErrorReporter().errorInProtocol(httpResponse.getStatusLine().toString());
        }
        QueryServiceResponse response;
        try (InputStream contentStream = httpResponse.getEntity().getContent()) {
            response = driverContext.genericObjectReader.readValue(contentStream, QueryServiceResponse.class);
        }
        QueryServiceResponse.Status status = response.status;
        if (httpStatus == HttpStatus.SC_OK && status == QueryServiceResponse.Status.SUCCESS) {
            return response;
        }
        if (status == QueryServiceResponse.Status.TIMEOUT) {
            throw getErrorReporter().errorTimeout();
        }
        SQLException exc = getErrorIfExists(response);
        if (exc != null) {
            throw exc;
        } else {
            throw getErrorReporter().errorInProtocol(httpResponse.getStatusLine().toString());
        }
    }

    JsonParser fetchResult(QueryServiceResponse response) throws SQLException {
        if (response.handle == null) {
            throw getErrorReporter().errorInProtocol();
        }
        int p = response.handle.lastIndexOf("/");
        if (p < 0) {
            throw getErrorReporter().errorInProtocol(response.handle);
        }
        String handlePath = response.handle.substring(p);
        URI resultRequestURI;
        try {
            resultRequestURI = new URI(queryResultEndpoint + handlePath);
        } catch (URISyntaxException e) {
            throw getErrorReporter().errorInProtocol(handlePath);
        }
        HttpGet httpGet = new HttpGet(resultRequestURI);
        httpGet.setHeader(HttpHeaders.ACCEPT, ContentType.APPLICATION_JSON.getMimeType());

        CloseableHttpResponse httpResponse = null;
        InputStream httpContentStream = null;
        JsonParser parser = null;
        try {
            httpResponse = httpClient.execute(httpGet, httpClientContext);
            int httpStatus = httpResponse.getStatusLine().getStatusCode();
            if (httpStatus != HttpStatus.SC_OK) {
                throw getErrorReporter().errorNoResult();
            }
            HttpEntity entity = httpResponse.getEntity();
            httpContentStream = entity.getContent();
            parser = driverContext.genericObjectReader
                    .createParser(new InputStreamWithAttachedResource(httpContentStream, httpResponse));
            if (!advanceToArrayField(parser, RESULTS)) {
                throw getErrorReporter().errorInProtocol();
            }
            parser.configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
            return parser;
        } catch (SQLException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw e;
        } catch (JsonProcessingException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw getErrorReporter().errorInProtocol(e);
        } catch (IOException e) {
            closeQuietly(e, parser, httpContentStream, httpResponse);
            throw getErrorReporter().errorInConnection(e);
        }
    }

    private boolean advanceToArrayField(JsonParser parser, String fieldName) throws IOException {
        if (parser.nextToken() != JsonToken.START_OBJECT) {
            return false;
        }
        for (;;) {
            JsonToken token = parser.nextValue();
            if (token == null || token == JsonToken.END_OBJECT) {
                return false;
            }
            if (parser.currentName().equals(fieldName)) {
                return token == JsonToken.START_ARRAY;
            } else if (token.isStructStart()) {
                parser.skipChildren();
            } else {
                parser.nextToken();
            }
        }
    }

    ArrayNode fetchExplainOnlyResult(QueryServiceResponse response,
            ADBPreparedStatement.AbstractValueSerializer stringSer) throws SQLException {
        if (response.results == null || response.results.isEmpty()) {
            throw getErrorReporter().errorInProtocol();
        }
        Object v = response.results.get(0);
        if (!(v instanceof String)) {
            throw getErrorReporter().errorInProtocol();
        }
        try (BufferedReader br = new BufferedReader(new StringReader(v.toString()))) {
            ArrayNode arrayNode = (ArrayNode) driverContext.genericObjectReader.createArrayNode();
            String line;
            while ((line = br.readLine()) != null) {
                arrayNode.addObject().put(ADBProtocol.EXPLAIN_ONLY_RESULT_COLUMN_NAME,
                        stringSer.serializeToString(line));
            }
            return arrayNode;
        } catch (IOException e) {
            throw getErrorReporter().errorInResultHandling(e);
        }
    }

    private HttpClientContext createHttpClientContext(URI uri) {
        HttpClientContext hcCtx = HttpClientContext.create();
        AuthCache ac = new BasicAuthCache();
        ac.put(new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme()), new BasicScheme());
        hcCtx.setAuthCache(ac);
        return hcCtx;
    }

    boolean isStatementCategory(QueryServiceResponse response, QueryServiceResponse.StatementCategory category) {
        return response.plans != null && category.equals(response.plans.statementCategory);
    }

    int getUpdateCount(QueryServiceResponse response) {
        // TODO:need to get update count through the response
        return isStatementCategory(response, QueryServiceResponse.StatementCategory.UPDATE) ? 1 : 0;
    }

    SQLException getErrorIfExists(QueryServiceResponse response) {
        if (response.errors != null && !response.errors.isEmpty()) {
            QueryServiceResponse.Message err = response.errors.get(0);
            return new SQLException(err.msg, null, err.code);
        }
        return null;
    }

    List<QueryServiceResponse.Message> getWarningIfExists(QueryServiceResponse response) {
        return response.warnings != null && !response.warnings.isEmpty() ? response.warnings : null;
    }

    SQLWarning createSQLWarning(List<QueryServiceResponse.Message> warnings) {
        SQLWarning sqlWarning = null;
        ListIterator<QueryServiceResponse.Message> i = warnings.listIterator(warnings.size());
        while (i.hasPrevious()) {
            QueryServiceResponse.Message w = i.previous();
            SQLWarning sw = new SQLWarning(w.msg, null, w.code);
            if (sqlWarning != null) {
                sw.setNextWarning(sqlWarning);
            }
            sqlWarning = sw;
        }
        return sqlWarning;
    }

    List<ADBColumn> getColumns(QueryServiceResponse response) throws SQLException {
        if (isExplainOnly(response)) {
            return Collections.singletonList(new ADBColumn(EXPLAIN_ONLY_RESULT_COLUMN_NAME, ADBDatatype.STRING, false));
        }
        QueryServiceResponse.Signature signature = response.signature;
        if (signature == null) {
            throw getErrorReporter().errorInProtocol();
        }
        List<String> nameList = signature.name;
        List<String> typeList = signature.type;
        if (nameList == null || nameList.isEmpty() || typeList == null || typeList.isEmpty()) {
            throw getErrorReporter().errorBadResultSignature();
        }
        int count = nameList.size();
        List<ADBColumn> result = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String columnName = nameList.get(i);
            String typeName = typeList.get(i);
            boolean optional = false;
            if (typeName.endsWith(OPTIONAL_TYPE_SUFFIX)) {
                optional = true;
                typeName = typeName.substring(0, typeName.length() - OPTIONAL_TYPE_SUFFIX.length());
            }
            ADBDatatype columnType = ADBDatatype.findByTypeName(typeName);
            if (columnType == null) {
                throw getErrorReporter().errorBadResultSignature();
            }
            result.add(new ADBColumn(columnName, columnType, optional));
        }
        return result;
    }

    boolean isExplainOnly(QueryServiceResponse response) {
        return response.plans != null && Boolean.TRUE.equals(response.plans.explainOnly);
    }

    int getStatementParameterCount(QueryServiceResponse response) throws SQLException {
        QueryServiceResponse.Plans plans = response.plans;
        if (plans == null) {
            throw getErrorReporter().errorInProtocol();
        }
        if (plans.statementParameters == null) {
            return 0;
        }
        int paramPos = 0;
        for (Object param : plans.statementParameters) {
            if (param instanceof Number) {
                paramPos = Math.max(paramPos, ((Number) param).intValue());
            } else {
                throw getErrorReporter().errorParameterNotSupported(String.valueOf(param));
            }
        }
        return paramPos;
    }

    JsonParser createJsonParser(JsonNode node) {
        return driverContext.genericObjectReader.treeAsTokens(node);
    }

    ADBErrorReporter getErrorReporter() {
        return driverContext.errorReporter;
    }

    Logger getLogger() {
        return driverContext.logger;
    }

    static ObjectMapper createObjectMapper() {
        ObjectMapper om = new ObjectMapper();
        om.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.NON_PRIVATE);
        // serialization
        om.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        // deserialization
        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        om.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        om.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
        return om;
    }

    private static void closeQuietly(Exception mainExc, java.io.Closeable... closeableList) {
        for (Closeable closeable : closeableList) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    if (mainExc != null) {
                        mainExc.addSuppressed(e);
                    }
                }
            }
        }
    }

    static final class ByteArrayOutputStreamImpl extends ByteArrayOutputStream implements ContentProducer {
        private ByteArrayOutputStreamImpl(int size) {
            super(size);
        }
    }

    static final class EntityTemplateImpl extends EntityTemplate {

        private final long contentLength;

        private EntityTemplateImpl(ByteArrayOutputStreamImpl baos, ContentType contentType) {
            super(baos);
            contentLength = baos.size();
            setContentType(contentType.toString());
        }

        @Override
        public long getContentLength() {
            return contentLength;
        }
    }

    static final class InputStreamWithAttachedResource extends FilterInputStream {

        private final Closeable resource;

        private InputStreamWithAttachedResource(InputStream delegate, Closeable resource) {
            super(delegate);
            this.resource = Objects.requireNonNull(resource);
        }

        @Override
        public void close() throws IOException {
            try {
                super.close();
            } finally {
                resource.close();
            }
        }
    }

    public static class QueryServiceResponse {

        public Status status;
        public Plans plans;
        public Signature signature;
        public String handle;
        public List<?> results; // currently only used for EXPLAIN results
        public List<Message> errors;
        public List<Message> warnings;

        public enum Status {
            RUNNING,
            SUCCESS,
            TIMEOUT,
            FAILED,
            FATAL
        }

        public static class Signature {
            List<String> name;
            List<String> type;
        }

        public static class Plans {
            StatementCategory statementCategory;
            List<Object> statementParameters;
            Boolean explainOnly;
        }

        public static class Message {
            int code;
            String msg;
        }

        public enum StatementCategory {
            QUERY,
            UPDATE,
            DDL,
            PROCEDURE
        }
    }
}
