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
package org.apache.asterix.common.functions;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * Checks on the parts of an external HTTP function's definition: its URL, its {@code path} option, its URL
 * templates and its headers.
 */
public final class HttpFunctionUtil {

    private static final String SCHEME_HTTP = "http";
    private static final String SCHEME_HTTPS = "https";
    private static final String AUTHORITY_PREFIX = "://";
    private static final char TEMPLATE_START = '{';
    private static final char TEMPLATE_END = '}';
    // stands in for each {name} so that a template parses as a URI; any unreserved character would do
    private static final String TEMPLATE_PLACEHOLDER = "0";
    private static final String PATH_SEPARATOR = "/";
    private static final String CURRENT_SEGMENT = ".";
    private static final String PARENT_SEGMENT = "..";
    // RFC 3986 unreserved characters other than letters and digits
    private static final String PATH_UNRESERVED_SYMBOLS = "-._~";
    private static final char DELETE_CHAR = 0x7f;
    // RFC 9110 delimiters, which a header name, a token, cannot contain
    private static final String HEADER_NAME_DELIMITERS = "\"(),/:;<=>?@[\\]{}";

    private HttpFunctionUtil() {
    }

    /**
     * @return the names of the {@code {name}} references in {@code template}, in order of appearance
     * @throws IllegalArgumentException if a reference is empty, unterminated or nested, or a '}' stands alone
     */
    public static List<String> getTemplateParameters(String template) {
        return parseTemplate(template).parameters();
    }

    /**
     * Checks an inline URL, which may hold {@code {name}} references in its path only: it must be absolute http or
     * https with a host, and carry no user info, query or fragment, since arguments and credentials have their own
     * places.
     *
     * @return the names of the {@code {name}} references in {@code url}, in order of appearance
     */
    public static List<String> validateUrl(String url, SourceLocation sourceLoc) throws CompilationException {
        ParsedTemplate template;
        URI uri;
        try {
            template = parseTemplate(url);
            uri = new URI(template.withPlaceholders());
        } catch (IllegalArgumentException | URISyntaxException e) {
            throw invalidUrl(url, sourceLoc, e.getMessage());
        }
        String scheme = uri.getScheme();
        if (!SCHEME_HTTP.equalsIgnoreCase(scheme) && !SCHEME_HTTPS.equalsIgnoreCase(scheme)) {
            throw invalidUrl(url, sourceLoc, "only http and https URLs are supported");
        }
        if (uri.getHost() == null) {
            // URI parses an authority whose host it rejects (e.g. one with '_') as registry-based, leaving no host
            String authority = uri.getRawAuthority();
            throw invalidUrl(url, sourceLoc,
                    authority == null ? "missing host" : "invalid host in '" + authority + "'");
        }
        if (uri.getRawUserInfo() != null) {
            throw invalidUrl(url, sourceLoc, "user info is not allowed; give credentials in the WITH clause");
        }
        if (uri.getRawQuery() != null) {
            throw invalidUrl(url, sourceLoc, "a query is not allowed; arguments are sent as parameters");
        }
        if (uri.getRawFragment() != null) {
            throw invalidUrl(url, sourceLoc, "a fragment is not allowed");
        }
        // a reference in the host or port would let any caller choose the server the cluster sends a request to
        int authorityStart = scheme.length() + AUTHORITY_PREFIX.length();
        int pathStart = url.indexOf(PATH_SEPARATOR, authorityStart);
        String authority = pathStart < 0 ? url.substring(authorityStart) : url.substring(authorityStart, pathStart);
        if (authority.indexOf(TEMPLATE_START) >= 0) {
            throw invalidUrl(url, sourceLoc, "a parameter reference is only allowed in the path");
        }
        return template.parameters();
    }

    /**
     * @return whether {@code url}, one that {@link #validateUrl} accepts, uses https
     */
    public static boolean isHttps(String url) {
        String prefix = SCHEME_HTTPS + AUTHORITY_PREFIX;
        return url.regionMatches(true, 0, prefix, 0, prefix.length());
    }

    /**
     * Checks a {@code path} option, which may hold {@code {name}} references. Outside them, a segment may hold only
     * letters, digits and {@code -._~}, and may not be {@code .} or {@code ..}, so the path can never resolve above
     * the link's base URL. Characters are allowed rather than forbidden because servers rewrite paths in more ways
     * than a deny-list anticipates: dropping what follows a {@code ;}, decoding twice, or decoding overlong UTF-8.
     *
     * @return the names of the {@code {name}} references in {@code path}, in order of appearance
     */
    public static List<String> validatePath(String path, SourceLocation sourceLoc) throws CompilationException {
        if (!path.startsWith(PATH_SEPARATOR)) {
            throw invalidPath(path, sourceLoc, "must start with '" + PATH_SEPARATOR + "'");
        }
        ParsedTemplate template;
        try {
            template = parseTemplate(path);
        } catch (IllegalArgumentException e) {
            throw invalidPath(path, sourceLoc, e.getMessage());
        }
        String[] segments = template.withPlaceholders().substring(1).split(PATH_SEPARATOR, -1);
        int lastSegment = segments.length - 1;
        for (int i = 0; i <= lastSegment; i++) {
            String segment = segments[i];
            // only the last segment may be empty, which keeps "/" and a trailing '/' but refuses "//"
            if (segment.isEmpty() && i < lastSegment) {
                throw invalidPath(path, sourceLoc, "must not contain an empty segment");
            }
            if (segment.equals(CURRENT_SEGMENT) || segment.equals(PARENT_SEGMENT)) {
                throw invalidPath(path, sourceLoc,
                        "must not contain '" + CURRENT_SEGMENT + "' or '" + PARENT_SEGMENT + "' segments");
            }
            for (int j = 0; j < segment.length(); j++) {
                char c = segment.charAt(j);
                if (!isPathUnreserved(c)) {
                    throw invalidPath(path, sourceLoc, "must not contain '" + c + "'; only letters, digits and '"
                            + PATH_UNRESERVED_SYMBOLS + "' are allowed outside parameter references");
                }
            }
        }
        return template.parameters();
    }

    private static boolean isPathUnreserved(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
                || PATH_UNRESERVED_SYMBOLS.indexOf(c) >= 0;
    }

    /**
     * @return whether {@code name} is a valid header name: non-empty ASCII with no whitespace, control character or
     *         delimiter
     */
    public static boolean isValidHeaderName(String name) {
        return !name.isEmpty()
                && name.chars().noneMatch(c -> c <= ' ' || c >= DELETE_CHAR || HEADER_NAME_DELIMITERS.indexOf(c) >= 0);
    }

    /**
     * @return whether {@code value} is a valid header value: a control character other than a tab is not allowed,
     *         and a CR or LF would also let it inject further headers
     */
    public static boolean isValidHeaderValue(String value) {
        return value.chars().noneMatch(c -> (c < ' ' && c != '\t') || c == DELETE_CHAR);
    }

    // checks and substitutes in one pass, so no loop over the template depends on a caller having checked it first
    private static ParsedTemplate parseTemplate(String template) {
        List<String> parameters = new ArrayList<>();
        StringBuilder withPlaceholders = new StringBuilder(template.length());
        int pos = 0;
        while (pos < template.length()) {
            char c = template.charAt(pos);
            if (c == TEMPLATE_START) {
                int end = template.indexOf(TEMPLATE_END, pos + 1);
                if (end < 0) {
                    throw new IllegalArgumentException("unterminated parameter reference");
                }
                String name = template.substring(pos + 1, end);
                if (name.isEmpty()) {
                    throw new IllegalArgumentException("empty parameter reference");
                }
                if (name.indexOf(TEMPLATE_START) >= 0) {
                    throw new IllegalArgumentException("nested parameter reference");
                }
                parameters.add(name);
                withPlaceholders.append(TEMPLATE_PLACEHOLDER);
                pos = end + 1;
            } else if (c == TEMPLATE_END) {
                throw new IllegalArgumentException("unexpected '" + TEMPLATE_END + "'");
            } else {
                withPlaceholders.append(c);
                pos++;
            }
        }
        return new ParsedTemplate(parameters, withPlaceholders.toString());
    }

    private static CompilationException invalidUrl(String url, SourceLocation sourceLoc, String reason) {
        return new CompilationException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL, sourceLoc, url, reason);
    }

    private static CompilationException invalidPath(String path, SourceLocation sourceLoc, String reason) {
        return new CompilationException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_OPTION, sourceLoc,
                HttpFunctionConstants.OPTION_PATH, "'" + path + "' " + reason);
    }

    /**
     * @param parameters the names of the template's references, in order of appearance
     * @param withPlaceholders the template with each reference replaced by {@link #TEMPLATE_PLACEHOLDER}
     */
    private record ParsedTemplate(List<String> parameters, String withPlaceholders) {
    }
}
