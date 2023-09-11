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

package org.apache.asterix.external.util;

import static org.apache.asterix.external.util.ExternalDataConstants.COMPUTED_FIELD_PATTERN;
import static org.apache.asterix.external.util.ExternalDataConstants.PREFIX_DEFAULT_DELIMITER;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.IWarningCollector;
import org.apache.hyracks.api.exceptions.Warning;
import org.apache.hyracks.util.LogRedactionUtil;

public final class ExternalDataPrefix implements Serializable {
    private static final long serialVersionUID = -7612997190679310483L;
    private final String original;
    private final String protocolContainerPair;
    private final boolean endsWithSlash;
    private final List<String> segments;

    private final List<String> computedFieldNames = new ArrayList<>();
    private final List<IAType> computedFieldTypes = new ArrayList<>();
    private final List<Integer> computedFieldSegmentIndexes = new ArrayList<>();
    private final List<ARecordType> paths = new ArrayList<>();
    private final Map<Integer, PrefixSegment> indexToComputedFieldsMap = new HashMap<>();
    private String root;

    public static final String PREFIX_ROOT_FIELD_NAME = "prefix-root";
    public static final Set<ATypeTag> supportedTypes = new HashSet<>();

    static {
        supportedTypes.add(ATypeTag.STRING);
        supportedTypes.add(ATypeTag.BIGINT);
        supportedTypes.add(ATypeTag.DOUBLE);
    }

    public ExternalDataPrefix(Map<String, String> configuration) throws AlgebricksException {
        String prefix = ExternalDataUtils.getDefinitionOrPath(configuration);
        this.original = prefix != null ? prefix : "";
        this.endsWithSlash = this.original.endsWith("/");
        protocolContainerPair = ExternalDataUtils.getProtocolContainerPair(configuration);
        segments = extractPrefixSegments(original);
        extractComputedFields();
        extractRoot();
    }

    public String getOriginal() {
        return original;
    }

    public boolean isEndsWithSlash() {
        return endsWithSlash;
    }

    public String getRoot() {
        return root;
    }

    public boolean hasComputedFields() {
        return !computedFieldNames.isEmpty();
    }

    public List<String> getSegments() {
        return segments;
    }

    public List<String> getComputedFieldNames() {
        return computedFieldNames;
    }

    public List<IAType> getComputedFieldTypes() {
        return computedFieldTypes;
    }

    public List<Integer> getComputedFieldSegmentIndexes() {
        return computedFieldSegmentIndexes;
    }

    public List<ARecordType> getPaths() {
        return paths;
    }

    public Map<Integer, PrefixSegment> getIndexToComputedFieldsMap() {
        return indexToComputedFieldsMap;
    }

    /**
     * extracts the segments of a prefix, separated by the delimiter
     */
    public static List<String> extractPrefixSegments(String prefix) {
        return prefix.isEmpty() ? Collections.emptyList() : Arrays.asList(prefix.split(PREFIX_DEFAULT_DELIMITER));
    }

    /**
     * extracts and returns the computed fields and their indexes from the provided prefix
     */
    private void extractComputedFields() throws AlgebricksException {
        // check if there are any segments before doing any testing
        if (!segments.isEmpty()) {
            // search for computed fields in each segment
            Matcher matcher = COMPUTED_FIELD_PATTERN.matcher("");

            // we need to keep track of the end position
            StringBuilder expression = new StringBuilder();

            for (int i = 0; i < segments.size(); i++) {
                matcher.reset(segments.get(i));
                expression.setLength(0);
                int end = 0;

                while (matcher.find()) {
                    expression.append(segments.get(i), end, matcher.start());

                    String computedField = matcher.group();
                    String[] splits = computedField.split(":");
                    String namePart = splits[0].substring(1);
                    String typePart = splits[1].substring(0, splits[1].length() - 1);

                    IAType type = BuiltinTypeMap.getBuiltinType(typePart);
                    type = getUpdatedType(type);
                    validateSupported(type.getTypeTag());

                    computedFieldNames.add(namePart);
                    computedFieldTypes.add(type);
                    computedFieldSegmentIndexes.add(i);
                    updateIndexToComputedFieldMap(i, namePart, type);

                    List<String> nameParts = List.of(namePart.split("\\."));
                    paths.add(ProjectionFiltrationTypeUtil.getPathRecordType(nameParts));

                    expression.append("(.+)");
                    end = matcher.end();
                }

                if (expression.length() > 0) {
                    expression.append(segments.get(i).substring(end));
                    indexToComputedFieldsMap.get(i).setExpression(expression.toString());
                }
            }
        }
    }

    private void updateIndexToComputedFieldMap(int segmentIndex, String computedFieldName, IAType computedFieldType) {
        if (indexToComputedFieldsMap.containsKey(segmentIndex)) {
            PrefixSegment prefixSegment = indexToComputedFieldsMap.get(segmentIndex);
            prefixSegment.getComputedFieldNames().add(computedFieldName);
            prefixSegment.getComputedFieldTypes().add(computedFieldType);
        } else {
            PrefixSegment prefixSegment = new PrefixSegment();
            prefixSegment.getComputedFieldNames().add(computedFieldName);
            prefixSegment.getComputedFieldTypes().add(computedFieldType);
            indexToComputedFieldsMap.put(segmentIndex, prefixSegment);
        }
    }

    /**
     * Returns the longest static path (root) before encountering the first computed field
     */
    private void extractRoot() {
        // check if there are any computed fields before doing any testing
        if (computedFieldNames.isEmpty()) {
            root = original;
            return;
        }

        StringBuilder builder = new StringBuilder();

        // construct all static parts before encountering the first computed field
        for (int i = 0; i < computedFieldSegmentIndexes.get(0); i++) {
            builder.append(segments.get(i)).append("/");
        }

        // remove last "/" and append it only if needed
        root = builder.toString();
        root = root.substring(0, root.length() - 1);
        root = ExternalDataUtils.appendSlash(root, endsWithSlash);
    }

    /**
     * Checks whether the provided type is in the supported types for dynamic prefixes
     *
     * @param type type to check
     * @throws CompilationException exception if type is not supported
     */
    private void validateSupported(ATypeTag type) throws CompilationException {
        if (!supportedTypes.contains(type)) {
            throw new CompilationException(ErrorCode.UNSUPPORTED_COMPUTED_FIELD_TYPE, type);
        }
    }

    public List<String> getValues(String key) {
        return extractValues(extractPrefixSegments(key));
    }

    /**
     * Evaluates whether the provided key satisfies the conditions of the evaluator or not
     * TODO Check if {@link IExternalFilterEvaluator#isComputedFieldUsed(int)} is useful once we have regex extractor
     *
     * @param key       ke
     * @param evaluator evaluator
     * @return true if key satisfies the evaluator conditions, false otherwise
     */
    public boolean evaluate(String key, IExternalFilterEvaluator evaluator, IWarningCollector warningCollector)
            throws HyracksDataException {
        // TODO provide the List to avoid array creation
        List<String> keySegments = extractPrefixSegments(key);

        // no computed fields filter, accept path
        if (!hasComputedFields() || evaluator.isEmpty()) {
            return true;
        }

        // segments of object key have to be larger than segments of the prefix
        if (keySegments.size() <= segments.size()) {
            return false;
        }

        // no computed fields used in WHERE clause, accept object
        if (evaluator.isEmpty()) {
            return true;
        }

        // extract values for all compute fields and set them in the evaluator
        // TODO provide the List to avoid array creation
        List<String> values = extractValues(keySegments);

        String computedFieldName = null;
        IAType computedFieldType = null;
        String computedFieldValue = null;
        try {
            for (int i = 0; i < computedFieldNames.size(); i++) {
                computedFieldName = computedFieldNames.get(i);
                computedFieldType = computedFieldTypes.get(i);
                computedFieldValue = values.get(i);

                if (evaluator.isComputedFieldUsed(i)) {
                    evaluator.setValue(i, computedFieldValue);
                }
            }
        } catch (NumberFormatException ex) {
            if (warningCollector.shouldWarn()) {
                warningCollector.warn(Warning.of(null, ErrorCode.FAILED_TO_EVALUATE_COMPUTED_FIELD,
                        LogRedactionUtil.userData(key), computedFieldName, computedFieldType,
                        LogRedactionUtil.userData(computedFieldValue), LogRedactionUtil.userData(ex.getMessage())));
            }
            return false;
        }

        return evaluator.evaluate();
    }

    public String removeProtocolContainerPair(String path) {
        return path.replace(protocolContainerPair, "");
    }

    public static boolean containsComputedFields(Map<String, String> configuration) {
        String path = ExternalDataUtils.getDefinitionOrPath(configuration);
        return path != null && path.contains("{");
    }

    /**
     * extracts the computed fields values from the object's key
     *
     * @param keySegments object's key segments
     * @return list of computed field values
     */
    private List<String> extractValues(List<String> keySegments) {
        List<String> values = new ArrayList<>();

        for (Map.Entry<Integer, PrefixSegment> entry : indexToComputedFieldsMap.entrySet()) {
            int index = entry.getKey();
            String expression = entry.getValue().getExpression();

            String keySegment = keySegments.get(index);
            Matcher matcher = Pattern.compile(expression).matcher(keySegment);

            if (matcher.find()) {
                for (int i = 1; i <= matcher.groupCount(); i++) {
                    values.add(matcher.group(i));
                }
            }
        }

        return values;
    }

    private IAType getUpdatedType(IAType type) {
        switch (type.getTypeTag()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return BuiltinType.AINT64;
            default:
                return type;
        }
    }

    public static class PrefixSegment implements Serializable {
        private static final long serialVersionUID = 8788939199985336347L;
        private String expression;
        private final List<String> computedFieldNames = new ArrayList<>();
        private final List<IAType> computedFieldTypes = new ArrayList<>();

        public PrefixSegment() {
        }

        public String getExpression() {
            return expression;
        }

        public List<String> getComputedFieldNames() {
            return computedFieldNames;
        }

        public List<IAType> getComputedFieldTypes() {
            return computedFieldTypes;
        }

        public void setExpression(String expression) {
            this.expression = expression;
        }
    }
}
