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
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.security.cert.CertificateException;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLPeerUnverifiedException;

import org.apache.asterix.common.api.INcApplicationContext;
import org.apache.asterix.common.config.ExternalProperties;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpEndpoint;
import org.apache.asterix.common.functions.HttpFunctionBodyFormat;
import org.apache.asterix.common.functions.HttpFunctionConstants;
import org.apache.asterix.common.functions.HttpFunctionMethod;
import org.apache.asterix.common.functions.HttpFunctionOnError;
import org.apache.asterix.common.functions.HttpFunctionRateLimiter;
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
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.ParseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.client.utils.DateUtils;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.hyracks.algebricks.data.IPrinter;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IError;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.api.util.ErrorMessageUtil;
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
 * string of a GET or in the body of a POST, as a JSON object, a URL-encoded form or a multipart form. A JSON response becomes the value it holds, and a text
 * response a string. A request the endpoint throttles is sent again a few times, within the call's timeout.
 */
class ExternalScalarHttpFunctionEvaluator extends ExternalScalarFunctionEvaluator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, String>> HEADERS_TYPE = new TypeReference<>() {
    };
    private static final String PATH_SEPARATOR = "/";
    private static final String QUERY_START = "?";
    private static final String NO_SEPARATOR = "";
    private static final String FORM_FIELD_SEPARATOR = "&";
    private static final String FORM_NAME_SEPARATOR = "=";
    private static final String CR = "\r";
    private static final String LF = "\n";
    private static final String CRLF = CR + LF;
    private static final String MULTIPART_DASHES = "--";
    private static final String MULTIPART_BOUNDARY = "boundary";
    private static final String MULTIPART_PART_HEADERS = "Content-Disposition: form-data; name=\"%s\"" + CRLF
            + "Content-Type: text/plain; charset=UTF-8" + CRLF + CRLF;
    private static final String QUOTE = "\"";
    private static final String ESCAPED_QUOTE = "%22";
    private static final String ESCAPED_CR = "%0D";
    private static final String ESCAPED_LF = "%0A";
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
    private static final String INVALID_HEADERS = "not a JSON object of strings";
    private static final int HEADER_SEPARATOR_AND_LINE_END_LENGTH = ": \r\n".length();
    private static final int READ_BUFFER_SIZE = 8192;
    private static final long NO_RETRY_AFTER = -1;
    private static final Set<IError> NULL_ON_ERRORS = Set.of(ErrorCode.EXTERNAL_HTTP_FUNCTION_REQUEST_FAILED,
            ErrorCode.EXTERNAL_HTTP_FUNCTION_SIZE_LIMIT_EXCEEDED, ErrorCode.EXTERNAL_HTTP_FUNCTION_HTTP_ERROR,
            ErrorCode.EXTERNAL_HTTP_FUNCTION_TIMEOUT, ErrorCode.EXTERNAL_HTTP_FUNCTION_RATE_LIMITED);

    private final INcApplicationContext appCtx;
    private final SourceLocation sourceLoc;
    private final Map<String, String> requestHeaders;
    private final boolean nullCall;
    private final boolean nullOnError;
    private final IWarningCollector warningCollector;
    private final HttpFunctionRateLimiter rateLimiter;
    private final HttpFunctionMethod method;
    private final HttpFunctionBodyFormat argumentFormat;
    private final ContentType bodyContentType;
    private final String multipartEnd;
    private final boolean urlencode;
    private final int timeoutSeconds;
    private final long timeoutNanos;
    private final RequestConfig requestConfig;
    private final int maxRequestSize;
    private final int maxResponseSize;
    private final int maxRetries;
    private final long firstRetryDelayNanos;
    private final String[] parameterPrefixes;
    private final IPointable[] argValues;
    private final String[] argTexts;
    private final IPrinter jsonPrinter;
    private final ByteArrayAccessibleOutputStream textBuffer = new ByteArrayAccessibleOutputStream();
    private final PrintStream textStream = new PrintStream(textBuffer, false, StandardCharsets.UTF_8);
    private final ByteArrayAccessibleOutputStream bodyBuffer = new ByteArrayAccessibleOutputStream();
    private final PrintStream bodyStream = new PrintStream(bodyBuffer, false, StandardCharsets.UTF_8);
    private final StringBuilder urlBuilder = new StringBuilder();
    private final StringBuilder formBuilder = new StringBuilder();
    private final byte[] readBuffer = new byte[READ_BUFFER_SIZE];
    private final ByteArrayAccessibleOutputStream responseBuffer = new ByteArrayAccessibleOutputStream();
    private final ByteArrayAccessibleInputStream responseInput = new ByteArrayAccessibleInputStream(new byte[0], 0, 0);
    private final JSONDataParser responseParser;
    private final UTF8StringWriter utf8Writer = new UTF8StringWriter();
    private final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    private final ResponseHandler<Outcome> responseHandler = this::readResponse;
    // whether the request being sent is sent again, with another token, if its endpoint answers 401
    private boolean retryOnUnauthorized;
    // whether the request being sent carries a token that replaced one its endpoint rejected
    private boolean replacementToken;
    // whether the endpoint rejected the replacement token too, which fails the query whatever on-error says
    private boolean tokenRejected;
    private boolean retryOnThrottled;
    // the System.nanoTime() by which the call must end, moved on by each wait for the node's rate limiter
    private long deadlineNanos;
    private int throttledStatus;
    private long retryAfterNanos;

    // resolved by the first call that sends a request
    private CloseableHttpClient client;
    private HttpFunctionNodeTokenSource tokenSource;
    private HttpClientContext context;
    private HttpFunctionUrlTemplate urlTemplate;
    private Header[] headers;
    private long headersSize;

    ExternalScalarHttpFunctionEvaluator(IExternalFunctionInfo finfo, IScalarEvaluatorFactory[] args, IAType[] argTypes,
            IEvaluatorContext ctx, SourceLocation sourceLoc, Map<String, String> requestHeaders)
            throws HyracksDataException {
        super(finfo, args, argTypes, ctx);
        this.sourceLoc = sourceLoc;
        this.requestHeaders = requestHeaders;
        appCtx = (INcApplicationContext) ctx.getServiceContext().getApplicationContext();
        ExternalProperties externalProperties = appCtx.getExternalProperties();
        Map<String, String> resources = finfo.getResources();
        nullCall = finfo.getNullCall();
        nullOnError = HttpFunctionOnError.valueOf(resources.get(HttpFunctionConstants.OPTION_ON_ERROR))
                == HttpFunctionOnError.NULL;
        warningCollector = ctx.getWarningCollector();
        rateLimiter = appCtx.getHttpFunctionRateLimiter();
        method = HttpFunctionMethod.valueOf(resources.get(HttpFunctionConstants.OPTION_METHOD));
        urlencode = Boolean.parseBoolean(resources.get(HttpFunctionConstants.OPTION_URLENCODE));
        String timeout = resources.get(HttpFunctionConstants.OPTION_TIMEOUT);
        timeoutSeconds = (int) Math.min(
                timeout == null ? externalProperties.getExternalHttpFunctionTimeout() : Long.parseLong(timeout),
                externalProperties.getExternalHttpFunctionMaxTimeout());
        timeoutNanos = TimeUnit.SECONDS.toNanos(timeoutSeconds);
        requestConfig = HttpFunctionConnectionManager.createRequestConfig(timeoutSeconds);
        maxRequestSize = externalProperties.getExternalHttpFunctionMaxRequestSize();
        maxResponseSize = externalProperties.getExternalHttpFunctionMaxResponseSize();
        maxRetries = externalProperties.getExternalHttpFunctionMaxRetries();
        firstRetryDelayNanos = TimeUnit.MILLISECONDS.toNanos(externalProperties.getExternalHttpFunctionRetryDelay());
        argumentFormat = method == HttpFunctionMethod.GET ? HttpFunctionBodyFormat.URLENCODED
                : HttpFunctionBodyFormat.valueOf(resources.get(HttpFunctionConstants.OPTION_BODY));
        String boundary = UUID.randomUUID().toString();
        bodyContentType = switch (argumentFormat) {
            case JSON -> ContentType.APPLICATION_JSON;
            case URLENCODED -> ContentType.APPLICATION_FORM_URLENCODED.withCharset(StandardCharsets.UTF_8);
            case FORMDATA -> ContentType.create(ContentType.MULTIPART_FORM_DATA.getMimeType(),
                    new BasicNameValuePair(MULTIPART_BOUNDARY, boundary));
        };
        String multipartDelimiter = MULTIPART_DASHES + boundary;
        multipartEnd = multipartDelimiter + MULTIPART_DASHES + CRLF;
        List<String> parameterNames = finfo.getParameterNames();
        parameterPrefixes = new String[args.length];
        for (int i = 0; i < args.length; i++) {
            String name = parameterNames.get(i);
            parameterPrefixes[i] = switch (argumentFormat) {
                case JSON -> JSONUtil.quoteAndEscape(name) + JSON_NAME_SEPARATOR;
                case URLENCODED -> URLEncoder.encode(name, StandardCharsets.UTF_8) + FORM_NAME_SEPARATOR;
                case FORMDATA -> multipartDelimiter + CRLF
                        + String.format(MULTIPART_PART_HEADERS, escapeMultipartName(name));
            };
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
        deadlineNanos = System.nanoTime() + timeoutNanos;
        String token = tokenSource == null ? null : tokenSource.getToken(deadlineNanos, null);
        if (!send(token, token != null)) {
            // a token can be revoked before it expires, so a rejected one is worth replacing once
            send(tokenSource.getToken(deadlineNanos, token), false);
        }
        result.set(resultStorage);
    }

    private void resolveEndpoint() throws HyracksDataException {
        Map<String, String> resources = finfo.getResources();
        HttpEndpoint endpoint = appCtx.getHttpEndpointResolver().resolve(finfo.getExternalIdentifier(), resources);
        String path = resources.get(HttpFunctionConstants.OPTION_PATH);
        String template =
                path == null ? endpoint.getUrl() : StringUtils.stripEnd(endpoint.getUrl(), PATH_SEPARATOR) + path;
        urlTemplate = new HttpFunctionUrlTemplate(template, finfo.getParameterNames(), sourceLoc);
        headers = mergeHeaders(endpoint.getHeaders(), requestHeaders,
                resources.get(HttpFunctionConstants.OPTION_HEADERS), sourceLoc);
        headersSize = getHeadersSize(headers);
        context = HttpFunctionConnectionManager.createContext(endpoint.getCertificate(), sourceLoc);
        client = appCtx.getHttpFunctionClient();
        tokenSource = endpoint.getOAuthClient() == null ? null
                : new HttpFunctionNodeTokenSource(endpoint, appCtx.getServiceContext(), sourceLoc, timeoutSeconds);
    }

    static Header[] mergeHeaders(Map<String, String> endpointHeaders, Map<String, String> requestHeaders,
            String functionHeaders, SourceLocation sourceLoc) throws HyracksDataException {
        // a function's header never replaces one of its endpoint's or its query's, whatever the case of its name
        Map<String, String> merged = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        merged.putAll(endpointHeaders);
        merged.putAll(requestHeaders);
        if (functionHeaders != null) {
            try {
                OBJECT_MAPPER.readValue(functionHeaders, HEADERS_TYPE).forEach(merged::putIfAbsent);
            } catch (JsonProcessingException e) {
                // not chained: the parser's message can quote the header values, which may be secrets
                throw new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION, sourceLoc,
                        HttpFunctionConstants.OPTION_HEADERS, INVALID_HEADERS);
            }
        }
        return merged.entrySet().stream().map(header -> new BasicHeader(header.getKey(), header.getValue()))
                .toArray(Header[]::new);
    }

    private HttpRequestBase createRequest(String token) throws HyracksDataException {
        for (int i = 0; i < argValues.length; i++) {
            argTexts[i] = urlTemplate.references(i) && !isNullOrMissing(i) ? getArgumentText(i) : null;
        }
        urlBuilder.setLength(0);
        urlTemplate.fill(urlBuilder, argTexts);
        HttpRequestBase request;
        int bodySize = 0;
        if (method == HttpFunctionMethod.GET) {
            appendForm(urlBuilder, QUERY_START);
            request = new HttpGet(toUri());
        } else {
            writeBody();
            bodySize = bodyBuffer.size();
            HttpPost post = new HttpPost(toUri());
            post.setEntity(new ByteArrayEntity(bodyBuffer.getByteArray(), 0, bodySize, bodyContentType));
            request = post;
        }
        request.setConfig(requestConfig);
        request.setHeaders(headers);
        long requestHeadersSize = headersSize;
        if (token != null) {
            String authorization = HttpAuthorization.bearer(token);
            request.setHeader(HttpHeaders.AUTHORIZATION, authorization);
            requestHeadersSize += getHeaderSize(HttpHeaders.AUTHORIZATION, authorization);
        }
        // the whole request, whatever its method: its URL, its headers and its body
        checkRequestSize((long) urlBuilder.length() + requestHeadersSize + bodySize);
        return request;
    }

    private static long getHeadersSize(Header[] headers) {
        long size = 0;
        for (Header header : headers) {
            size += getHeaderSize(header.getName(), header.getValue());
        }
        return size;
    }

    // a header is sent as its name, ": ", its value and a line end
    private static long getHeaderSize(String name, String value) {
        return name.length() + value.length() + HEADER_SEPARATOR_AND_LINE_END_LENGTH;
    }

    // only with null-call, since a NULL or MISSING argument is otherwise the result
    private boolean isNullOrMissing(int argIndex) {
        byte tag = argValues[argIndex].getByteArray()[argValues[argIndex].getStartOffset()];
        return tag == ATypeTag.SERIALIZED_NULL_TYPE_TAG || tag == ATypeTag.SERIALIZED_MISSING_TYPE_TAG;
    }

    private void appendForm(StringBuilder out, String firstSeparator) throws HyracksDataException {
        String separator = firstSeparator;
        for (int i = 0; i < argValues.length; i++) {
            if (!urlTemplate.references(i)) {
                String text = getArgumentText(i);
                out.append(separator).append(parameterPrefixes[i])
                        .append(urlencode ? URLEncoder.encode(text, StandardCharsets.UTF_8) : text);
                separator = FORM_FIELD_SEPARATOR;
            }
        }
    }

    private void writeBody() throws HyracksDataException {
        bodyBuffer.reset();
        switch (argumentFormat) {
            case JSON -> writeJsonBody();
            case URLENCODED -> {
                formBuilder.setLength(0);
                appendForm(formBuilder, NO_SEPARATOR);
                bodyStream.print(formBuilder);
            }
            case FORMDATA -> writeMultipartBody();
        }
        bodyStream.flush();
    }

    private void writeMultipartBody() throws HyracksDataException {
        for (int i = 0; i < argValues.length; i++) {
            if (!urlTemplate.references(i)) {
                bodyStream.print(parameterPrefixes[i]);
                bodyStream.print(getArgumentText(i));
                bodyStream.print(CRLF);
            }
        }
        bodyStream.print(multipartEnd);
    }

    // a quoted multipart field name escapes what would end it, as browsers do
    private static String escapeMultipartName(String name) {
        return name.replace(QUOTE, ESCAPED_QUOTE).replace(CR, ESCAPED_CR).replace(LF, ESCAPED_LF);
    }

    private void writeJsonBody() throws HyracksDataException {
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
    }

    // for a URL, a form field or a multipart part, none of which is JSON: a value that JSON writes as a string, a
    // date or a uuid as much as a string, is sent as that string's value, without JSON's quotes or escapes, and any
    // other value as its JSON text. A JSON body does not use this: it writes every value as JSON (writeJsonBody).
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

    /**
     * Sends the call's request, again while its endpoint throttles it and retries are left. With on-error NULL, a
     * failure of the endpoint, or of reaching it, makes the result NULL, with a warning.
     *
     * @return {@code false} if the endpoint answered 401 and {@code retryOnUnauthorized} is set, else {@code true},
     *         with the result in {@link #resultStorage}
     */
    private boolean send(String token, boolean retryOnUnauthorized) throws HyracksDataException {
        this.retryOnUnauthorized = retryOnUnauthorized;
        replacementToken = token != null && !retryOnUnauthorized;
        tokenRejected = false;
        try {
            HttpRequestBase request = createRequest(token);
            for (int retries = 0;; retries++) {
                retryOnThrottled = retries < maxRetries;
                waitForRateLimiter();
                Outcome outcome = HttpFunctionConnectionManager.execute(client, request, context, deadlineNanos,
                        responseHandler, sourceLoc, urlTemplate.getDisplayUrl(), timeoutSeconds);
                if (outcome != Outcome.THROTTLED) {
                    return outcome == Outcome.RESULT;
                }
                waitToRetry(retries);
            }
        } catch (HyracksDataException e) {
            if (!nullOnError || tokenRejected || !isNullOnError(e)) {
                throw e;
            }
            warnNull(e);
            resultStorage.assign(PointableHelper.NULL_REF);
            return true;
        }
    }

    // the wait is the node's, not the endpoint's: it does not count against the call's timeout, so a busy node never
    // fails a call, or makes it NULL; cancelling the query still interrupts it
    private void waitForRateLimiter() throws HyracksDataException {
        long waitNanos = rateLimiter.reserve(System.nanoTime());
        sleep(waitNanos);
        deadlineNanos += waitNanos;
    }

    private void waitToRetry(int retries) throws HyracksDataException {
        long delayNanos;
        if (retryAfterNanos == NO_RETRY_AFTER) {
            long backoffNanos = firstRetryDelayNanos << retries;
            delayNanos = backoffNanos + ThreadLocalRandom.current().nextLong(backoffNanos / 2 + 1);
        } else {
            delayNanos = retryAfterNanos;
        }
        if (delayNanos > deadlineNanos - System.nanoTime()) {
            throw rateLimited("the endpoint answered " + throttledStatus + " and the retry would wait "
                    + TimeUnit.NANOSECONDS.toMillis(delayNanos) + " ms");
        }
        sleep(delayNanos);
    }

    private static void sleep(long nanos) throws HyracksDataException {
        try {
            TimeUnit.NANOSECONDS.sleep(nanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw HyracksDataException.create(e);
        }
    }

    private HyracksDataException rateLimited(String reason) {
        return new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_RATE_LIMITED, sourceLoc,
                urlTemplate.getDisplayUrl(), timeoutSeconds, reason);
    }

    // a certificate the endpoint failed always fails the query too: which servers to trust is part of the function's
    // definition, as its URL is. So does cancelling it, which the client can report as a failed request.
    private static boolean isNullOnError(HyracksDataException e) {
        return !Thread.currentThread().isInterrupted() && e.getError().filter(NULL_ON_ERRORS::contains).isPresent()
                && ExceptionUtils.indexOfType(e, CertificateException.class) < 0
                && ExceptionUtils.indexOfType(e, SSLPeerUnverifiedException.class) < 0;
    }

    private void warnNull(HyracksDataException e) {
        if (warningCollector.shouldWarn()) {
            // the failure's own message without its source location, which the warning shows
            String failure = ErrorMessageUtil.formatMessage(e.getComponent(), e.getErrorCode(),
                    e.getError().get().errorMessage(), null, e.getParams());
            warningCollector.warn(Warning.of(sourceLoc, ErrorCode.EXTERNAL_HTTP_FUNCTION_NULL_ON_ERROR,
                    finfo.getFunctionIdentifier().getName(), failure));
        }
    }

    private Outcome readResponse(HttpResponse response) throws IOException {
        int status = response.getStatusLine().getStatusCode();
        if (status == HttpStatus.SC_UNAUTHORIZED) {
            if (retryOnUnauthorized) {
                return Outcome.UNAUTHORIZED;
            }
            // a fresh token rejected too points at the definition, as a token that cannot be got does; were it NULL,
            // each row would get the token replaced again
            tokenRejected = replacementToken;
        }
        if (retryOnThrottled && isThrottled(status)) {
            throttledStatus = status;
            retryAfterNanos = getRetryAfterNanos(response.getFirstHeader(HttpHeaders.RETRY_AFTER));
            return Outcome.THROTTLED;
        }
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
            // a success with nothing to return, as a 201, 202 or 204 to a POST often is, whatever its Content-Type
            resultStorage.getDataOutput().writeByte(ATypeTag.SERIALIZED_NULL_TYPE_TAG);
            return Outcome.RESULT;
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
        return Outcome.RESULT;
    }

    // a 503 may answer a POST its endpoint has acted on, which sending it again would repeat
    private boolean isThrottled(int status) {
        return status == HttpStatus.SC_TOO_MANY_REQUESTS
                || (status == HttpStatus.SC_SERVICE_UNAVAILABLE && method == HttpFunctionMethod.GET);
    }

    // a Retry-After is a number of seconds or an HTTP date
    private static long getRetryAfterNanos(Header retryAfter) {
        if (retryAfter == null) {
            return NO_RETRY_AFTER;
        }
        String value = retryAfter.getValue().trim();
        if (StringUtils.isNumeric(value)) {
            return TimeUnit.SECONDS.toNanos(NumberUtils.toLong(value, Long.MAX_VALUE));
        }
        Date date = DateUtils.parseDate(value);
        return date == null ? NO_RETRY_AFTER
                : TimeUnit.MILLISECONDS.toNanos(Math.max(0, date.getTime() - System.currentTimeMillis()));
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

    // what the response to a request asks of the call that sent it
    private enum Outcome {
        // the result is in resultStorage
        RESULT,
        // send it again with another token
        UNAUTHORIZED,
        // send it again later
        THROTTLED
    }
}
