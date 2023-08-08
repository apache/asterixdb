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
import static org.apache.asterix.external.util.ExternalDataConstants.DEFINITION_FIELD_NAME;
import static org.apache.asterix.external.util.ExternalDataConstants.KEY_PATH;
import static org.apache.asterix.external.util.ExternalDataConstants.PREFIX_DEFAULT_DELIMITER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

import org.apache.asterix.common.exceptions.CompilationException;
import org.apache.asterix.common.exceptions.ErrorCode;
import org.apache.asterix.common.external.IExternalFilterEvaluator;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ExternalDataPrefix {

    private final String original;
    private String root;
    private final boolean endsWithSlash;
    private final List<String> segments;

    private final List<String> computedFieldNames = new ArrayList<>();
    private final List<IAType> computedFieldTypes = new ArrayList<>();
    private final List<Integer> computedFieldSegmentIndexes = new ArrayList<>();
    private final List<ARecordType> paths = new ArrayList<>();
    private final Map<Integer, Pair<List<String>, List<IAType>>> computedFields = new HashMap<>();

    public static final String PREFIX_ROOT_FIELD_NAME = "prefix-root";
    public static final Set<ATypeTag> supportedTypes = new HashSet<>();

    static {
        supportedTypes.add(BuiltinType.ASTRING.getTypeTag());
        supportedTypes.add(BuiltinType.AINT32.getTypeTag());
    }

    public ExternalDataPrefix(Map<String, String> configuration) throws AlgebricksException {
        this(getDefinitionOrPath(configuration));
    }

    public ExternalDataPrefix(String prefix) throws AlgebricksException {
        this.original = prefix != null ? prefix : "";
        this.endsWithSlash = this.original.endsWith("/");

        segments = extractPrefixSegments(original);
        extractComputedFields();
        extractRoot();

        for (int i = 0; i < computedFieldSegmentIndexes.size(); i++) {
            int segmentIndex = computedFieldSegmentIndexes.get(i);

            if (computedFields.containsKey(segmentIndex)) {
                Pair<List<String>, List<IAType>> pair = computedFields.get(segmentIndex);
                pair.getLeft().add(computedFieldNames.get(i));
                pair.getRight().add(computedFieldTypes.get(i));
            } else {
                List<String> names = new ArrayList<>();
                List<IAType> types = new ArrayList<>();

                names.add(computedFieldNames.get(i));
                types.add(computedFieldTypes.get(i));
                computedFields.put(segmentIndex, Pair.of(names, types));
            }
        }
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

    /**
     * extracts the segments of a prefix, separated by the delimiter
     */
    private List<String> extractPrefixSegments(String prefix) {
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
            for (int i = 0; i < segments.size(); i++) {
                matcher.reset(segments.get(i));

                while (matcher.find()) {
                    String computedField = matcher.group();
                    String[] splits = computedField.split(":");
                    String namePart = splits[0].substring(1);
                    String typePart = splits[1].substring(0, splits[1].length() - 1);

                    IAType type = BuiltinTypeMap.getBuiltinType(typePart);
                    validateSupported(type.getTypeTag());

                    computedFieldNames.add(namePart);
                    computedFieldTypes.add(type);
                    computedFieldSegmentIndexes.add(i);

                    List<String> nameParts = List.of(namePart.split("\\."));
                    paths.add(ProjectionFiltrationTypeUtil.getPathRecordType(nameParts));
                }
            }
        }
    }

    /**
     * Returns the longest static path (root) before encountering the first computed field
     */
    private void extractRoot() {
        StringBuilder builder = new StringBuilder();

        // check if there are any computed fields before doing any testing
        if (computedFieldNames.isEmpty()) {
            root = original;
            return;
        }

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

    /**
     * Evaluates whether the provided key satisfies the conditions of the evaluator or not
     * TODO Check if {@link IExternalFilterEvaluator#isComputedFieldUsed(int)} is useful once we have regex extractor
     *
     * @param key       ke
     * @param evaluator evaluator
     * @return true if key satisfies the evaluator conditions, false otherwise
     */
    public boolean evaluate(String key, IExternalFilterEvaluator evaluator) throws HyracksDataException {
        // TODO provide the List to avoid array creation
        List<String> keySegments = extractPrefixSegments(key);

        // segments of object key have to be larger than segments of the prefix
        if (keySegments.size() <= segments.size()) {
            return false;
        }

        // extract values for all compute fields and set them in the evaluator
        // TODO provide the List to avoid array creation
        List<String> values = extractValues(keySegments);
        for (int i = 0; i < computedFieldNames.size(); i++) {
            evaluator.setValue(i, values.get(i));
        }

        return evaluator.evaluate();
    }

    /**
     * extracts the computed fields values from the object's key
     *
     * @param keySegments object's key segments
     * @return list of computed field values
     */
    private List<String> extractValues(List<String> keySegments) {
        List<String> values = new ArrayList<>();

        for (Integer computedFieldSegmentIndex : computedFieldSegmentIndexes) {
            values.add(keySegments.get(computedFieldSegmentIndex));
        }

        return values;
    }

    private static String getDefinitionOrPath(Map<String, String> configuration) {
        return configuration.getOrDefault(DEFINITION_FIELD_NAME, configuration.get(KEY_PATH));
    }
}
