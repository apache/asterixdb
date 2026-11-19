/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.asterix.app.external;

import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_BEARER_TOKEN;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_BODY;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_CERTIFICATE;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_HEADERS;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_METHOD;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_OAUTH_ALLOWED_SCOPES;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_OAUTH_CLIENT_ID;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_OAUTH_CLIENT_SECRET;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_OAUTH_TOKEN_URI;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_ON_ERROR;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_PASSWORD;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_PATH;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_TIMEOUT;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_URLENCODE;
import static org.apache.asterix.common.functions.HttpFunctionConstants.OPTION_USERNAME;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.functions.HttpFunctionBodyFormat;
import org.apache.asterix.common.functions.HttpFunctionConstants;
import org.apache.asterix.common.functions.HttpFunctionMethod;
import org.apache.asterix.common.functions.HttpFunctionOnError;
import org.apache.asterix.common.functions.HttpFunctionUtil;
import org.apache.asterix.common.metadata.DataverseName;
import org.apache.asterix.lang.common.statement.CreateFunctionStatement;
import org.apache.asterix.object.base.AdmBigIntNode;
import org.apache.asterix.object.base.AdmBooleanNode;
import org.apache.asterix.object.base.AdmObjectNode;
import org.apache.asterix.object.base.AdmStringNode;
import org.apache.asterix.object.base.IAdmNode;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.http.HttpHeaders;
import org.apache.hyracks.api.exceptions.SourceLocation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The stored form of an external HTTP function: the external identifier that names its target, and the resources
 * that hold its validated options. Every option is checked here, at CREATE time, so that a stored definition is
 * always one the runtime can call.
 */
public final class HttpFunctionDefinition {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final boolean URLENCODE_DEFAULT = true;
    private static final String CERTIFICATE_TYPE = "X.509";
    private static final String OPTION_LIST_SEPARATOR = ", ";
    // without TLS, a credential crosses the network readable by anyone on the path
    private static final String HTTPS_REQUIRED = "sends a credential, so the URL must use https";

    private static final Set<String> COMMON_OPTIONS =
            Set.of(OPTION_METHOD, OPTION_BODY, OPTION_HEADERS, OPTION_TIMEOUT, OPTION_URLENCODE, OPTION_ON_ERROR,
                    CreateFunctionStatement.NULLCALL_FIELD_NAME, CreateFunctionStatement.DETERMINISTIC_FIELD_NAME);
    // a link carries its own base URL and authentication, so only a path below it may be given
    private static final Set<String> LINK_OPTIONS = Set.of(OPTION_PATH);
    private static final List<AuthScheme> AUTH_SCHEMES =
            List.of(new AuthScheme(List.of(OPTION_USERNAME, OPTION_PASSWORD), List.of()),
                    new AuthScheme(List.of(OPTION_BEARER_TOKEN), List.of()),
                    new AuthScheme(List.of(OPTION_OAUTH_TOKEN_URI, OPTION_OAUTH_CLIENT_ID, OPTION_OAUTH_CLIENT_SECRET),
                            List.of(OPTION_OAUTH_ALLOWED_SCOPES)));
    private static final Set<String> URL_OPTIONS = urlOptions();
    // the HTTP client sets these from the request itself; a definer's value would fail or redirect each call
    private static final Set<String> RUNTIME_HEADERS = caseInsensitiveSet(HttpHeaders.HOST, HttpHeaders.CONTENT_LENGTH,
            HttpHeaders.TRANSFER_ENCODING, HttpHeaders.CONNECTION, HttpHeaders.EXPECT, HttpHeaders.UPGRADE,
            HttpHeaders.TE, HttpHeaders.CONTENT_TYPE);

    private final List<String> externalIdentifier;
    private final Map<String, String> resources;

    private HttpFunctionDefinition(List<String> externalIdentifier, Map<String, String> resources) {
        this.externalIdentifier = externalIdentifier;
        this.resources = resources;
    }

    /**
     * Validates a function that names its URL inline, with any credentials it needs in its WITH clause. Credentials
     * and a certificate are accepted only with an https URL.
     *
     * @param url
     *            the URL, which may hold {@code {name}} references to the function's parameters
     * @param options
     *            the WITH clause, or {@code null} if there was none
     * @param paramNames
     *            the names of the function's parameters
     */
    public static HttpFunctionDefinition forUrl(String url, AdmObjectNode options, Collection<String> paramNames,
            SourceLocation sourceLoc) throws CompilationException {
        List<String> urlParameters = HttpFunctionUtil.validateUrl(url, sourceLoc);
        String unknownParameter = findUnknownParameter(urlParameters, paramNames);
        if (unknownParameter != null) {
            throw new CompilationException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL, sourceLoc, url,
                    unknownParameterReason(unknownParameter));
        }
        AdmObjectNode withOptions = orEmpty(options);
        Map<String, String> resources = resolveCommonOptions(withOptions, URL_OPTIONS, sourceLoc);
        String authOption = resolveAuthentication(withOptions, resources, sourceLoc);
        boolean hasAuthorizationHeader = hasHeader(withOptions, HttpHeaders.AUTHORIZATION);
        if (authOption != null && hasAuthorizationHeader) {
            throw invalidOption(OPTION_HEADERS,
                    "header '" + HttpHeaders.AUTHORIZATION + "' cannot be given with " + authOption, sourceLoc);
        }
        boolean isHttps = HttpFunctionUtil.isHttps(url);
        if (!isHttps && authOption != null) {
            throw invalidOption(authOption, HTTPS_REQUIRED, sourceLoc);
        }
        if (!isHttps && hasAuthorizationHeader) {
            throw invalidOption(OPTION_HEADERS, "header '" + HttpHeaders.AUTHORIZATION + "' " + HTTPS_REQUIRED,
                    sourceLoc);
        }
        String certificate = getNonEmptyString(withOptions, OPTION_CERTIFICATE, sourceLoc);
        if (certificate != null) {
            if (!isHttps) {
                throw invalidOption(OPTION_CERTIFICATE, "applies only to an https URL", sourceLoc);
            }
            validateCertificate(certificate, sourceLoc);
            resources.put(OPTION_CERTIFICATE, certificate);
        }
        return new HttpFunctionDefinition(List.of(HttpFunctionConstants.TARGET_URL, url), resources);
    }

    /**
     * Validates a function that names a link, whose base URL and authentication it uses.
     *
     * @param linkDatabaseName
     *            the database of the link
     * @param linkDataverseName
     *            the dataverse of the link
     * @param linkName
     *            the name of the link
     * @param options
     *            the WITH clause, or {@code null} if there was none
     * @param paramNames
     *            the names of the function's parameters
     */
    public static HttpFunctionDefinition forLink(String linkDatabaseName, DataverseName linkDataverseName,
            String linkName, AdmObjectNode options, Collection<String> paramNames, SourceLocation sourceLoc)
            throws CompilationException {
        AdmObjectNode withOptions = orEmpty(options);
        Map<String, String> resources = resolveCommonOptions(withOptions, LINK_OPTIONS, sourceLoc);
        if (hasHeader(withOptions, HttpHeaders.AUTHORIZATION)) {
            throw invalidOption(OPTION_HEADERS,
                    "header '" + HttpHeaders.AUTHORIZATION + "' cannot be given with a link", sourceLoc);
        }
        String path = getNonEmptyString(withOptions, OPTION_PATH, sourceLoc);
        if (path != null) {
            List<String> pathParameters = HttpFunctionUtil.validatePath(path, sourceLoc);
            String unknownParameter = findUnknownParameter(pathParameters, paramNames);
            if (unknownParameter != null) {
                throw invalidOption(OPTION_PATH, unknownParameterReason(unknownParameter), sourceLoc);
            }
            resources.put(OPTION_PATH, path);
        }
        return new HttpFunctionDefinition(List.of(HttpFunctionConstants.TARGET_LINK, linkDatabaseName,
                linkDataverseName.getCanonicalForm(), linkName), resources);
    }

    /**
     * @return {@code [url, <url>]} or {@code [link, <database>, <dataverse>, <link>]}
     */
    public List<String> getExternalIdentifier() {
        return externalIdentifier;
    }

    /**
     * @return the validated options, keyed by their {@code HttpFunctionConstants.OPTION_*} names, with each default
     *         resolved
     */
    public Map<String, String> getResources() {
        return resources;
    }

    private static Map<String, String> resolveCommonOptions(AdmObjectNode options, Set<String> targetOptions,
            SourceLocation sourceLoc) throws CompilationException {
        Set<String> unsupported = new TreeSet<>(options.getFieldNames());
        unsupported.removeAll(COMMON_OPTIONS);
        unsupported.removeAll(targetOptions);
        if (!unsupported.isEmpty()) {
            throw new CompilationException(ErrorCode.UNSUPPORTED_WITH_FIELD, sourceLoc,
                    String.join(OPTION_LIST_SEPARATOR, unsupported));
        }
        Map<String, String> resources = new HashMap<>();
        HttpFunctionMethod method =
                getEnum(options, OPTION_METHOD, HttpFunctionMethod.class, HttpFunctionMethod.GET, sourceLoc);
        resources.put(OPTION_METHOD, method.name());
        // a GET sends its arguments as a query string, which is form-encoded like a URLENCODED body
        HttpFunctionBodyFormat argumentFormat = HttpFunctionBodyFormat.URLENCODED;
        if (method == HttpFunctionMethod.POST) {
            argumentFormat =
                    getEnum(options, OPTION_BODY, HttpFunctionBodyFormat.class, HttpFunctionBodyFormat.JSON, sourceLoc);
            resources.put(OPTION_BODY, argumentFormat.name());
        } else if (options.contains(OPTION_BODY)) {
            throw invalidOption(OPTION_BODY, "only a " + HttpFunctionMethod.POST + " request has a body", sourceLoc);
        }
        if (argumentFormat == HttpFunctionBodyFormat.URLENCODED) {
            resources.put(OPTION_URLENCODE, String.valueOf(getUrlencode(options, sourceLoc)));
        } else if (options.contains(OPTION_URLENCODE)) {
            throw invalidOption(OPTION_URLENCODE, "only a " + HttpFunctionMethod.GET + " query string or a "
                    + HttpFunctionBodyFormat.URLENCODED + " body is URL-encoded", sourceLoc);
        }
        resources.put(OPTION_ON_ERROR,
                getEnum(options, OPTION_ON_ERROR, HttpFunctionOnError.class, HttpFunctionOnError.FAIL, sourceLoc)
                        .name());
        IAdmNode timeout = getTyped(options, OPTION_TIMEOUT, ATypeTag.BIGINT, sourceLoc);
        if (timeout != null) {
            long seconds = ((AdmBigIntNode) timeout).get();
            if (seconds <= 0) {
                throw invalidOption(OPTION_TIMEOUT, "must be a positive number of seconds", sourceLoc);
            }
            resources.put(OPTION_TIMEOUT, String.valueOf(seconds));
        }
        IAdmNode headers = getTyped(options, OPTION_HEADERS, ATypeTag.OBJECT, sourceLoc);
        if (headers != null) {
            resources.put(OPTION_HEADERS, resolveHeaders((AdmObjectNode) headers, sourceLoc));
        }
        return resources;
    }

    private static String resolveHeaders(AdmObjectNode headers, SourceLocation sourceLoc) throws CompilationException {
        // header names are case-insensitive, so two spellings of one name would be two values for one header
        Map<String, String> resolved = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, IAdmNode> header : headers.getFields()) {
            String name = header.getKey();
            IAdmNode value = header.getValue();
            if (!HttpFunctionUtil.isValidHeaderName(name)) {
                throw invalidOption(OPTION_HEADERS, "invalid header name '" + name + "'", sourceLoc);
            }
            if (RUNTIME_HEADERS.contains(name)) {
                throw invalidOption(OPTION_HEADERS, "header '" + name + "' is set by the runtime", sourceLoc);
            }
            if (value.getType() != ATypeTag.STRING) {
                throw invalidOption(OPTION_HEADERS,
                        "the value of header '" + name + "' must be a string, but found " + value.getType(), sourceLoc);
            }
            String headerValue = ((AdmStringNode) value).get();
            if (!HttpFunctionUtil.isValidHeaderValue(headerValue)) {
                throw invalidOption(OPTION_HEADERS, "the value of header '" + name + "' contains a control character",
                        sourceLoc);
            }
            if (resolved.put(name, headerValue) != null) {
                throw invalidOption(OPTION_HEADERS, "header '" + name + "' is given more than once", sourceLoc);
            }
        }
        try {
            return OBJECT_MAPPER.writeValueAsString(resolved);
        } catch (JsonProcessingException e) {
            // a map of strings always serializes
            throw new IllegalStateException(e);
        }
    }

    /**
     * @return the option that chose the authentication scheme, or {@code null} if none was given
     */
    private static String resolveAuthentication(AdmObjectNode options, Map<String, String> resources,
            SourceLocation sourceLoc) throws CompilationException {
        AuthScheme chosen = null;
        String chosenOption = null;
        for (AuthScheme scheme : AUTH_SCHEMES) {
            String givenOption = scheme.findGiven(options);
            if (givenOption == null) {
                continue;
            }
            if (chosen != null) {
                throw new CompilationException(ErrorCode.PARAMETERS_NOT_ALLOWED_AT_SAME_TIME, sourceLoc, chosenOption,
                        givenOption);
            }
            chosen = scheme;
            chosenOption = givenOption;
        }
        if (chosen == null) {
            return null;
        }
        for (String required : chosen.required()) {
            String value = getNonEmptyString(options, required, sourceLoc);
            if (value == null) {
                throw new CompilationException(ErrorCode.REQUIRED_PARAM_IF_PARAM_IS_PRESENT, sourceLoc, required,
                        chosenOption);
            }
            resources.put(required, value);
        }
        for (String optional : chosen.optional()) {
            String value = getNonEmptyString(options, optional, sourceLoc);
            if (value != null) {
                resources.put(optional, value);
            }
        }
        String tokenUri = resources.get(OPTION_OAUTH_TOKEN_URI);
        if (tokenUri != null) {
            if (!HttpFunctionUtil.validateUrl(tokenUri, sourceLoc).isEmpty()) {
                throw invalidOption(OPTION_OAUTH_TOKEN_URI, "must not reference parameters", sourceLoc);
            }
            if (!HttpFunctionUtil.isHttps(tokenUri)) {
                throw invalidOption(OPTION_OAUTH_TOKEN_URI, "the client secret is sent to it, so it must use https",
                        sourceLoc);
            }
        }
        return chosenOption;
    }

    private static void validateCertificate(String certificate, SourceLocation sourceLoc) throws CompilationException {
        Collection<?> certificates;
        try {
            certificates = CertificateFactory.getInstance(CERTIFICATE_TYPE)
                    .generateCertificates(new ByteArrayInputStream(certificate.getBytes(StandardCharsets.US_ASCII)));
        } catch (CertificateException e) {
            throw invalidOption(OPTION_CERTIFICATE,
                    "not a PEM-encoded " + CERTIFICATE_TYPE + " certificate: " + e.getMessage(), sourceLoc);
        }
        if (certificates.isEmpty()) {
            throw invalidOption(OPTION_CERTIFICATE, "holds no " + CERTIFICATE_TYPE + " certificate", sourceLoc);
        }
    }

    private static <E extends Enum<E>> E getEnum(AdmObjectNode options, String name, Class<E> type, E defaultValue,
            SourceLocation sourceLoc) throws CompilationException {
        IAdmNode value = getTyped(options, name, ATypeTag.STRING, sourceLoc);
        if (value == null) {
            return defaultValue;
        }
        String text = ((AdmStringNode) value).get();
        for (E constant : type.getEnumConstants()) {
            if (constant.name().equalsIgnoreCase(text)) {
                return constant;
            }
        }
        String allowed = Arrays.stream(type.getEnumConstants()).map(c -> c.name().toLowerCase(Locale.ROOT))
                .collect(Collectors.joining(OPTION_LIST_SEPARATOR));
        throw new CompilationException(ErrorCode.INVALID_PARAM_VALUE_ALLOWED_VALUE, sourceLoc, name, allowed);
    }

    private static boolean getUrlencode(AdmObjectNode options, SourceLocation sourceLoc) throws CompilationException {
        IAdmNode value = getTyped(options, OPTION_URLENCODE, ATypeTag.BOOLEAN, sourceLoc);
        return value == null ? URLENCODE_DEFAULT : ((AdmBooleanNode) value).get();
    }

    private static String getNonEmptyString(AdmObjectNode options, String name, SourceLocation sourceLoc)
            throws CompilationException {
        IAdmNode value = getTyped(options, name, ATypeTag.STRING, sourceLoc);
        if (value == null) {
            return null;
        }
        String text = ((AdmStringNode) value).get();
        if (text.isEmpty()) {
            throw invalidOption(name, "must not be empty", sourceLoc);
        }
        return text;
    }

    private static IAdmNode getTyped(AdmObjectNode options, String name, ATypeTag type, SourceLocation sourceLoc)
            throws CompilationException {
        IAdmNode value = options.get(name);
        if (value != null && value.getType() != type) {
            throw new CompilationException(ErrorCode.WITH_FIELD_MUST_BE_OF_TYPE, sourceLoc, name, type,
                    value.getType());
        }
        return value;
    }

    private static String findUnknownParameter(List<String> references, Collection<String> paramNames) {
        for (String parameter : references) {
            if (!paramNames.contains(parameter)) {
                return parameter;
            }
        }
        return null;
    }

    private static String unknownParameterReason(String parameter) {
        return "'" + parameter + "' is not a parameter of the function";
    }

    // resolveCommonOptions has already checked that the headers option, if given, is an object
    private static boolean hasHeader(AdmObjectNode options, String name) {
        AdmObjectNode headers = (AdmObjectNode) options.get(OPTION_HEADERS);
        return headers != null && headers.getFieldNames().stream().anyMatch(name::equalsIgnoreCase);
    }

    private static AdmObjectNode orEmpty(AdmObjectNode options) {
        return options == null ? AdmObjectNode.EMPTY : options;
    }

    private static CompilationException invalidOption(String name, String reason, SourceLocation sourceLoc) {
        return new CompilationException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION, sourceLoc, name, reason);
    }

    private static Set<String> caseInsensitiveSet(String... values) {
        Set<String> set = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        set.addAll(List.of(values));
        return Collections.unmodifiableSet(set);
    }

    private static Set<String> urlOptions() {
        Set<String> options = new HashSet<>();
        for (AuthScheme scheme : AUTH_SCHEMES) {
            options.addAll(scheme.required());
            options.addAll(scheme.optional());
        }
        options.add(OPTION_CERTIFICATE);
        return Collections.unmodifiableSet(options);
    }

    /**
     * One way of authenticating: all of {@code required} must be given together, and {@code optional} only with
     * them.
     */
    private record AuthScheme(List<String> required, List<String> optional) {

        /**
         * @return the first of this scheme's options that {@code options} gives, or {@code null} if it gives none
         */
        String findGiven(AdmObjectNode options) {
            List<String> all = new ArrayList<>(required);
            all.addAll(optional);
            return all.stream().filter(options::contains).findFirst().orElse(null);
        }
    }
}
