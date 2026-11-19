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
package org.apache.asterix.external.library;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpEndpoint;
import org.apache.asterix.common.functions.HttpFunctionConstants;
import org.apache.asterix.common.functions.HttpFunctionMethod;
import org.apache.asterix.dataflow.data.nontagged.printers.json.clean.AObjectPrinterFactory;
import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.external.parser.factory.JSONDataParserFactory;
import org.apache.asterix.external.provider.context.NoOpExternalRuntimeDataContext;
import org.apache.asterix.om.functions.IExternalFunctionInfo;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleInputStream;
import org.apache.hyracks.data.std.util.ByteArrayAccessibleOutputStream;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.JSONUtil;
import org.apache.hyracks.util.string.UTF8StringUtil;
import org.apache.hyracks.util.string.UTF8StringWriter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Calls an external HTTP function once per tuple. The arguments fill the URL template, and the rest go in the query
 * string of a GET or in the JSON object body of a POST. A JSON response becomes the value it holds, and a text
 * response a string.
 */
class ExternalScalarHttpFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {
    };
    private static final String PATH_SEPARATOR = "/";
    private static final char QUERY_START = '?';
    private static final char QUERY_PARAMETER_SEPARATOR = '&';
    private static final String QUERY_NAME_SEPARATOR = "=";
    private static final String JSON_STRING_START = "\"";
    private static final char JSON_OBJECT_START = '{';
    private static final char JSON_OBJECT_END = '}';
    private static final char JSON_FIELD_SEPARATOR = ',';
    private static final String JSON_NAME_SEPARATOR = ":";
    private static final String JSON_MIME_TYPE = "application/json";
    private static final String STRUCTURED_JSON_MIME_TYPE_PREFIX = "application/";
    private static final String STRUCTURED_JSON_MIME_TYPE_SUFFIX = "+json";
    private static final String TEXT_MIME_TYPE_PREFIX = "text/";
    private static final String REQUEST = "request";
    private static final String RESPONSE = "response";
    private static final String MALFORMED_JSON = "the response is not valid JSON";
    private static final int HEADER_SEPARATOR_AND_LINE_END_LENGTH = ": \r\n".length();
    private static final int READ_BUFFER_SIZE = 8192;

    private final INcApplicationContext appCtx;
    private final SourceLocation sourceLoc;
    private final boolean nullCall;
    private final HttpFunctionMethod method;
    private final boolean urlencode;
    private final int timeoutSeconds;
    private final long timeoutMillis;
    private final RequestConfig requestConfig;
    private final int maxRequestSize;
    private final int maxResponseSize;
    // each parameter's name as it starts a query parameter of a GET or a field of a POST body
    private final String[] parameterPrefixes;
    private final IPointable[] argValues;
    private final String[] argTexts;
    private final IPrinter jsonPrinter;
    private final ByteArrayAccessibleOutputStream textBuffer = new ByteArrayAccessibleOutputStream();
    private final PrintStream textStream = new PrintStream(textBuffer, false, StandardCharsets.UTF_8);
    private final ByteArrayAccessibleOutputStream bodyBuffer = new ByteArrayAccessibleOutputStream();
    private final PrintStream bodyStream = new PrintStream(bodyBuffer, false, StandardCharsets.UTF_8);
    private final StringBuilder urlBuilder = new StringBuilder();
    private final byte[] readBuffer = new byte[READ_BUFFER_SIZE];
    private final ByteArrayAccessibleOutputStream responseBuffer = new ByteArrayAccessibleOutputStream();
    private final ByteArrayAccessibleInputStream responseInput = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
    private final JSONDataParser responseParser;
    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final ResponseHandler<Void> responseHandler = this::readResponse;

    // resolved by the first call that sends a request
    private CloseableHttpClient client;
    private HttpFunctionUrlTemplate urlTemplate;
    private Header[] headers;

    ExternalScalarHttpFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args, IAType[] argTypes,
            IEvaluatorContext ctx, SourceLocation sourceLoc) throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        this.sourceLoc = sourceLoc;
        appCtx = (INcApplicationContext) ctx.getServiceContext().getApplicationContext();
        ExternalProperties externalProperties = appCtx.getExternalProperties();
        Map<String, String> resources = finfo.getResources();
        nullCall = finfo.getNullCall();
        method = HttpFunctionMethod.valueOf(resources.get(HttpFunctionConstants.OPTION_METHOD));
        urlencode = Boolean.parseBoolean(resources.get(HttpFunctionConstants.OPTION_URLENCODE));
        String timeout = resources.get(HttpFunctionConstants.OPTION_TIMEOUT);
        timeoutSeconds = (int) Math.min(
                timeout == null ? externalProperties.getExternalHttpFunctionTimeout() : Long.parseLong(timeout),
                externalProperties.getExternalHttpFunctionMaxTimeout());
        timeoutMillis = TimeUnit.SECONDS.toMillis(timeoutSeconds);
        int timeoutMillisInt = Math.toIntExact(timeoutMillis);
        requestConfig = RequestConfig.custom().setConnectTimeout(timeoutMillisInt)
                .setConnectionRequestTimeout(timeoutMillisInt).setSocketTimeout(timeoutMillisInt).build();
        maxRequestSize = externalProperties.getExternalHttpFunctionMaxRequestSize();
        maxResponseSize = externalProperties.getExternalHttpFunctionMaxResponseSize();
        List<String> parameterNames = finfo.getParameterNames();
        parameterPrefixes = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            String name = parameterNames.get(i);
            parameterPrefixes[i] = method == HttpFunctionMethod.GET
                    ? URLEncoder.encode(name, StandardCharsets.UTF_8) + QUERY_NAME_SEPARATOR
                    : JSONUtil.quoteAndEscape(name) + JSON_NAME_SEPARATOR;
        }
        argValues = new IPointable[args.length];
        for (int i = 0; i < args.length; i++) {
            argValues[i] = VoidPointable.FACTORY.createPointable();
        }
        argTexts = new String[args.length];
        jsonPrinter = AObjectPrinterFactory.INSTANCE.createPrinter(ctx);
        responseParser = (JSONDataParser) new JSONDataParserFactory()
                .createInputStreamParser(NoOpExternalRuntimeDataContext.INSTANCE);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        boolean hasNullArg = false;
        for (int i = 0, ln = argEvals.length; i < ln; i++) {
            argEvals[i].evaluate(tuple, argValues[i]);
            if (!nullCall) {
                ATypeTag argType =
                        ATYPETAGDESERIALIZER.deserialize(argValues[i].getByteArray()[argValues[i].getStartOffset()]);
                if (argType == ATypeTag.MISSING) {
                    PointableHelper.setMissing(result);
                    return;
                } else if (argType == ATypeTag.NULL) {
                    hasNullArg = true;
                }
            }
        }
        if (!nullCall && hasNullArg) {
            PointableHelper.setNull(result);
            return;
        }
        if (client == null) {
            resolveEndpoint();
        }
        send(createRequest());
        result.set(resultStorage);
    }

    private void resolveEndpoint() throws HyracksDataException {
        Map<String, String> resources = finfo.getResources();
        HttpEndpoint endpoint = appCtx.getHttpEndpointResolver().resolve(finfo.getExternalIdentifier(), resources);
        String path = resources.get(HttpFunctionConstants.OPTION_PATH);
        String template =
                path == null ? endpoint.getUrl() : StringUtils.stripEnd(endpoint.getUrl(), PATH_SEPARATOR) + path;
        urlTemplate = new HttpFunctionUrlTemplate(template, finfo.getParameterNames(), sourceLoc);
        headers = mergeHeaders(endpoint.getHeaders(), resources.get(HttpFunctionConstants.OPTION_HEADERS));
        client = appCtx.getHttpFunctionClient();
    }

    private static Header[] mergeHeaders(Map<String, String> endpointHeaders, String functionHeaders)
            throws HyracksDataException {
        // a function's header never replaces one of its endpoint's, whatever the case of its name
        Map<String, String> merged = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        merged.putAll(endpointHeaders);
        if (functionHeaders != null) {
            try {
                OBJECT_MAPPER.readValue(functionHeaders, HEADERS_TYPE).forEach(merged::putIfAbsent);
            } catch (JsonProcessingException e) {
                throw HyracksDataException.create(e);
            }
        }
        return merged.entrySet().stream().map(header -> new BasicHeader(header.getKey(), header.getValue()))
                .toArray(Header[]::new);
    }

    private HttpRequestBase createRequest() throws HyracksDataException {
        for (int i = 0; i < argValues.length; i++) {
            argTexts[i] = urlTemplate.references(i) ? getArgumentText(i) : null;
        }
        urlBuilder.setLength(0);
        urlTemplate.fill(urlBuilder, argTexts);
        HttpRequestBase request;
        int bodySize = 0;
        if (method == HttpFunctionMethod.GET) {
            appendQuery();
            request = new HttpGet(toUri());
        } else {
            writeBody();
            bodySize = bodyBuffer.size();
            HttpPost post = new HttpPost(toUri());
            post.setEntity(new ByteArrayEntity(bodyBuffer.getByteArray(), 0, bodySize, ContentType.APPLICATION_JSON));
            request = post;
        }
        request.setConfig(requestConfig);
        request.setHeaders(headers);
        // the whole request, whatever its method: its URL, its headers and its body
        checkRequestSize((long) urlBuilder.length() + getHeadersSize(request) + bodySize);
        return request;
    }

    // a header is sent as its name, ": ", its value and a line end
    private static long getHeadersSize(HttpRequestBase request) {
        long size = 0;
        for (Header header : request.getAllHeaders()) {
            size += header.getName().length() + header.getValue().length() + HEADER_SEPARATOR_AND_LINE_END_LENGTH;
        }
        return size;
    }

    private void appendQuery() throws HyracksDataException {
        char separator = QUERY_START;
        for (int i = 0; i < argValues.length; i++) {
            if (!urlTemplate.references(i)) {
                String text = getArgumentText(i);
                urlBuilder.append(separator).append(parameterPrefixes[i])
                        .append(urlencode ? URLEncoder.encode(text, StandardCharsets.UTF_8) : text);
                separator = QUERY_PARAMETER_SEPARATOR;
            }
        }
    }

    private void writeBody() throws HyracksDataException {
        bodyBuffer.reset();
        bodyStream.print(JSON_OBJECT_START);
        boolean first = true;
        for (int i = 0; i < argValues.length; i++) {
            if (!urlTemplate.references(i)) {
                if (!first) {
                    bodyStream.print(JSON_FIELD_SEPARATOR);
                }
                first = false;
                bodyStream.print(parameterPrefixes[i]);
                jsonPrinter.print(argValues[i].getByteArray(), argValues[i].getStartOffset(), argValues[i].getLength(),
                        bodyStream);
            }
        }
        bodyStream.print(JSON_OBJECT_END);
        bodyStream.flush();
    }

    // an argument is sent as its JSON text, except that a value JSON writes as a string, a string or a date or a
    // uuid alike, is sent as that string's value: quotes belong to JSON, not to a URL
    private String getArgumentText(int argIndex) throws HyracksDataException {
        IPointable arg = argValues[argIndex];
        byte[] bytes = arg.getByteArray();
        int start = arg.getStartOffset();
        if (bytes[start] == ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            return UTF8StringUtil.toString(bytes, start + 1);
        }
        textBuffer.reset();
        jsonPrinter.print(bytes, start, arg.getLength(), textStream);
        textStream.flush();
        String json = new String(textBuffer.getByteArray(), 0, textBuffer.size(), StandardCharsets.UTF_8);
        return json.startsWith(JSON_STRING_START) ? decodeJsonString(json) : json;
    }

    private static String decodeJsonString(String json) throws HyracksDataException {
        try {
            return OBJECT_MAPPER.readValue(json, String.class);
        } catch (JsonProcessingException e) {
            throw HyracksDataException.create(e);
        }
    }

    private void checkRequestSize(long size) throws HyracksDataException {
        if (size > maxRequestSize) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_SIZE_LIMIT_EXCEEDED, sourceLoc, REQUEST,
                    urlTemplate.getDisplayUrl(), maxRequestSize);
        }
    }

    private URI toUri() throws HyracksDataException {
        try {
            return new URI(urlBuilder.toString());
        } catch (URISyntaxException e) {
            // the reason alone, since the message would show the arguments
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL, sourceLoc,
                    urlTemplate.getDisplayUrl(), e.getReason());
        }
    }

    private void send(HttpRequestBase request) throws HyracksDataException {
        try {
            HttpFunctionConnectionManager.execute(client, request, timeoutMillis, responseHandler);
        } catch (HyracksDataException e) {
            // a refusal of the egress policy, or an error the response raised, already in its final form
            throw e;
        } catch (IOException e) {
            if (request.isAborted() || e instanceof SocketTimeoutException || e instanceof ConnectTimeoutException) {
                // not chained: an InterruptedIOException at the root of the cause would report the job as cancelled
                throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT, sourceLoc,
                        urlTemplate.getDisplayUrl(), timeoutSeconds);
            }
            throw requestFailed(e, ExceptionUtils.getMessage(e));
        }
    }

    private Void readResponse(HttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status < HttpStatus.SC_OK || status >= HttpStatus.SC_MULTIPLE_CHOICES) {
            throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_HTTP_ERROR, sourceLoc,
                    urlTemplate.getDisplayUrl(), status);
        }
        resultStorage.reset();
        HttpEntity entity = response.getEntity();
        responseBuffer.reset();
        if (entity != null) {
            readBody(entity.getContent());
        }
        if (responseBuffer.size() == 0) {
            resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            return null;
        }
        ContentType contentType = getContentType(entity);
        String mimeType = contentType == null ? null : contentType.getMimeType().toLowerCase(Locale.ROOT);
        Charset charset = contentType == null || contentType.getCharset() == null ? StandardCharsets.UTF_8
                : contentType.getCharset();
        if (mimeType == null) {
            throw requestFailed(null, "the response has no Content-Type");
        } else if (isJson(mimeType)) {
            parseJson(charset);
        } else if (mimeType.startsWith(TEXT_MIME_TYPE_PREFIX)) {
            DataOutput out = resultStorage.getDataOutput();
            out.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            utf8Writer.writeUTF8(new String(responseBuffer.getByteArray(), 0, responseBuffer.size(), charset), out);
        } else {
            throw requestFailed(null, "unsupported Content-Type '" + mimeType + "'");
        }
        return null;
    }

    // the stream is left open: closing it would read the rest of a body the cap has just refused. The client
    // releases the connection at the end of the body, and closes it otherwise.
    private void readBody(InputStream content) throws IOException {
        for (int read = content.read(readBuffer); read >= 0; read = content.read(readBuffer)) {
            if (read > maxResponseSize - responseBuffer.size()) {
                throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_SIZE_LIMIT_EXCEEDED, sourceLoc,
                        RESPONSE, urlTemplate.getDisplayUrl(), maxResponseSize);
            }
            responseBuffer.write(readBuffer, 0, read);
        }
    }

    private ContentType getContentType(HttpEntity entity) throws HyracksDataException {
        try {
            return ContentType.get(entity);
        } catch (ParseException | UnsupportedCharsetException e) {
            throw requestFailed(e, "invalid Content-Type: " + e.getMessage());
        }
    }

    private static boolean isJson(String mimeType) {
        return mimeType.equals(JSON_MIME_TYPE) || (mimeType.startsWith(STRUCTURED_JSON_MIME_TYPE_PREFIX)
                && mimeType.endsWith(STRUCTURED_JSON_MIME_TYPE_SUFFIX));
    }

    private void parseJson(Charset charset) throws HyracksDataException {
        if (charset.equals(StandardCharsets.UTF_8)) {
            responseInput.setContent(responseBuffer.getByteArray(), 0, responseBuffer.size());
        } else {
            // the parser reads UTF-8
            byte[] utf8 = new String(responseBuffer.getByteArray(), 0, responseBuffer.size(), charset)
                    .getBytes(StandardCharsets.UTF_8);
            responseInput.setContent(utf8, 0, utf8.length);
        }
        DataOutput out = resultStorage.getDataOutput();
        boolean oneValue;
        try {
            responseParser.reset(responseInput);
            oneValue = responseParser.parseAnyValue(out) && !responseParser.parseAnyValue(out);
        } catch (IOException e) {
            // not chained: the client is shown the deepest formatted error, which would be the parser's
            oneValue = false;
        }
        if (!oneValue) {
            throw requestFailed(null, MALFORMED_JSON);
        }
    }

    private HyracksDataException requestFailed(Throwable cause, String reason) {
        return new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED, cause, sourceLoc,
                urlTemplate.getDisplayUrl(), reason);
    }
}
