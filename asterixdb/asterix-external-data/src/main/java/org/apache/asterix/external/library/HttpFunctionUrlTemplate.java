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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.exceptions.RuntimeDataException;
import org.apache.asterix.common.functions.HttpFunctionUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;

/**
 * The URL of an external HTTP function, whose {@code {name}} references are filled with the function's arguments,
 * each as one path segment.
 * <p>
 * An argument that is empty, {@code .} or {@code ..}, or that contains {@code /} or {@code \}, is refused rather than
 * encoded: encoding leaves {@code .} as it is, and a server may decode an encoded separator back into one. Any other
 * argument is percent-encoded as a path segment, whatever the function's {@code urlencode} option says: a space as
 * {@code %20}, and {@code ?}, {@code #}, {@code %}, {@code +} and non-ASCII characters as their UTF-8 bytes, while
 * unreserved characters stay as they are.
 */
final class HttpFunctionUrlTemplate {

    private static final String SCHEME_SEPARATOR = "://";
    private static final String QUERY_OR_FRAGMENT_START = "?#";
    private static final char PATH_START = '/';
    private static final char USER_INFO_END = '@';
    private static final String REFERENCE_START = "{";
    private static final String REFERENCE_END = "}";
    private static final String CURRENT_SEGMENT = ".";
    private static final String PARENT_SEGMENT = "..";
    private static final char SEGMENT_SEPARATOR = '/';
    private static final char BACKSLASH = '\\';
    private static final String FORM_ENCODED_SPACE = "+";
    private static final String PERCENT_ENCODED_SPACE = "%20";
    private static final String TILDE = "~";
    private static final String PERCENT_ENCODED_TILDE = "%7E";

    private final List<String> parameterNames;
    private final SourceLocation sourceLoc;
    private final String displayUrl;
    private final String[] literals;
    private final int[] parameterIndexes;
    private final boolean[] referenced;

    /**
     * @param template the URL, with {@code {name}} references to the function's parameters
     * @param parameterNames the names of the function's parameters, in order
     * @throws HyracksDataException if the template is malformed or references a name that is not a parameter
     */
    HttpFunctionUrlTemplate(String template, List<String> parameterNames, SourceLocation sourceLoc)
            throws HyracksDataException {
        this.parameterNames = parameterNames;
        this.sourceLoc = sourceLoc;
        displayUrl = toDisplayUrl(template);
        List<String> references;
        try {
            references = HttpFunctionUtil.getTemplateParameters(template);
        } catch (IllegalArgumentException e) {
            throw invalidUrl(e.getMessage());
        }
        literals = new String[references.size() + 1];
        parameterIndexes = new int[references.size()];
        referenced = new boolean[parameterNames.size()];
        int position = 0;
        for (int i = 0; i < references.size(); i++) {
            String name = references.get(i);
            int parameterIndex = parameterNames.indexOf(name);
            if (parameterIndex < 0) {
                throw invalidUrl("'" + name + "' is not a parameter of the function");
            }
            String reference = REFERENCE_START + name + REFERENCE_END;
            int referenceStart = template.indexOf(reference, position);
            literals[i] = template.substring(position, referenceStart);
            parameterIndexes[i] = parameterIndex;
            referenced[parameterIndex] = true;
            position = referenceStart + reference.length();
        }
        literals[references.size()] = template.substring(position);
    }

    /**
     * @return the template as {@code scheme://host[:port]/path}, the only form of it that messages and logs show
     */
    String getDisplayUrl() {
        return displayUrl;
    }

    /**
     * @return whether the template references the parameter at {@code parameterIndex}
     */
    boolean references(int parameterIndex) {
        return referenced[parameterIndex];
    }

    /**
     * Appends the URL to {@code url}, with each reference replaced by the text of its argument.
     *
     * @param arguments the text of each argument, by parameter index. Only those the template references are read.
     * @throws HyracksDataException if an argument cannot stand as one path segment
     */
    void fill(StringBuilder url, String[] arguments) throws HyracksDataException {
        for (int i = 0; i < parameterIndexes.length; i++) {
            int parameterIndex = parameterIndexes[i];
            url.append(literals[i]).append(toSegment(parameterIndex, arguments[parameterIndex]));
        }
        url.append(literals[parameterIndexes.length]);
    }

    private String toSegment(int parameterIndex, String value) throws HyracksDataException {
        if (value.isEmpty() || value.equals(CURRENT_SEGMENT) || value.equals(PARENT_SEGMENT)
                || StringUtils.containsAny(value, SEGMENT_SEPARATOR, BACKSLASH)) {
            throw invalidArgument(parameterIndex, "must not be empty, '" + CURRENT_SEGMENT + "' or '" + PARENT_SEGMENT
                    + "', or contain '" + SEGMENT_SEPARATOR + "' or '" + BACKSLASH + "'");
        }
        // URLEncoder encodes a form field, in which a space is '+' and '~' is encoded; in a path, '+' is itself and
        // '~' is unreserved
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace(FORM_ENCODED_SPACE, PERCENT_ENCODED_SPACE)
                .replace(PERCENT_ENCODED_TILDE, TILDE);
    }

    // leaves out user info, query and fragment, which may hold credentials; a template holds no argument
    private static String toDisplayUrl(String template) {
        int authorityStart = template.indexOf(SCHEME_SEPARATOR) + SCHEME_SEPARATOR.length();
        int end = StringUtils.indexOfAny(template, QUERY_OR_FRAGMENT_START);
        String url = end < 0 ? template : template.substring(0, end);
        int pathStart = url.indexOf(PATH_START, authorityStart);
        int userInfoEnd = url.lastIndexOf(USER_INFO_END, pathStart < 0 ? url.length() : pathStart);
        return userInfoEnd < authorityStart ? url : url.substring(0, authorityStart) + url.substring(userInfoEnd + 1);
    }

    private HyracksDataException invalidUrl(String reason) {
        return new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_URL, sourceLoc, displayUrl, reason);
    }

    private HyracksDataException invalidArgument(int parameterIndex, String reason) {
        return new RuntimeDataException(ErrorCode.EXTERNAL_HTTP_FUNCTION_INVALID_ARGUMENT, sourceLoc,
                parameterNames.get(parameterIndex), reason);
    }
}
