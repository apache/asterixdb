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
import static org.apache.asterix.om.utils.ProjectionFiltrationTypeUtil.getRecordTypeWithFieldTypes;

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
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.BuiltinTypeMap;
import org.apache.asterix.om.types.IAType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;

public class ExternalDataPrefix {

    private final String original;
    private final String root;
    private final boolean endsWithSlash;

    private final List<String> segments;
    private final ComputedFieldDetails computedFieldDetails;

    public static final String PREFIX_ROOT_FIELD_NAME = "prefix-root";
    public static final Set<ATypeTag> supportedTypes = new HashSet<>();

    static {
        supportedTypes.add(BuiltinType.ASTRING.getTypeTag());
        supportedTypes.add(BuiltinType.AINT32.getTypeTag());
    }

    public ExternalDataPrefix(String prefix) throws AlgebricksException {
        this.original = prefix != null ? prefix : "";
        this.endsWithSlash = this.original.endsWith("/");

        this.segments = getPrefixSegments(this.original);

        computedFieldDetails = getComputedFields(segments);
        this.root = getPrefixRoot(segments, computedFieldDetails.getComputedFieldIndexes());
    }

    public String getOriginal() {
        return original;
    }

    public String getRoot() {
        return root;
    }

    public boolean isEndsWithSlash() {
        return endsWithSlash;
    }

    public List<String> getSegments() {
        return segments;
    }

    public ComputedFieldDetails getComputedFieldDetails() {
        return computedFieldDetails;
    }

    /**
     * returns the segments of a prefix, separated by the delimiter
     *
     * @param prefix prefix
     * @return an array of prefix segments
     */
    public static List<String> getPrefixSegments(String prefix) {
        return prefix.isEmpty() ? Collections.emptyList() : Arrays.asList(prefix.split(PREFIX_DEFAULT_DELIMITER));
    }

    /**
     * Extracts and returns the computed fields and their indexes from the provided prefix
     * @param prefix prefix
     *
     * @return Pair of computed field names and their segment index in the prefix
     */
    public static ComputedFieldDetails getComputedFields(String prefix) throws AlgebricksException {
        List<String> segments = getPrefixSegments(prefix);
        return getComputedFields(segments);
    }

    public static ComputedFieldDetails getComputedFields(List<String> segments) throws AlgebricksException {
        List<List<String>> computedFieldsNames = new ArrayList<>();
        List<IAType> computedFieldTypes = new ArrayList<>();
        List<Integer> computedFieldIndexes = new ArrayList<>();

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

                    List<String> nameParts = List.of(namePart.split("\\."));
                    computedFieldsNames.add(nameParts);
                    computedFieldTypes.add(type);
                    computedFieldIndexes.add(i);
                }
            }
        }

        return new ComputedFieldDetails(computedFieldsNames, computedFieldTypes, computedFieldIndexes);
    }

    /**
     * Checks whether the provided type is in the supported types for dynamic prefixes
     *
     * @param type type to check
     * @throws CompilationException exception if type is not supported
     */
    private static void validateSupported(ATypeTag type) throws CompilationException {
        if (!supportedTypes.contains(type)) {
            throw new CompilationException(ErrorCode.UNSUPPORTED_COMPUTED_FIELD_TYPE, type);
        }
    }

    /**
     * Returns the longest static path (root) before encountering the first computed field
     *
     * @param prefix prefix
     * @return prefix root
     */
    public String getPrefixRoot(String prefix) throws AlgebricksException {
        List<String> prefixSegments = getPrefixSegments(prefix);
        List<Integer> computedFieldIndexes = getComputedFields(prefix).getComputedFieldIndexes();
        return getPrefixRoot(prefixSegments, computedFieldIndexes);
    }

    public String getPrefixRoot(List<String> prefixSegments, List<Integer> computedFieldIndexes) {
        StringBuilder root = new StringBuilder();

        // check if there are any computed fields before doing any testing
        if (computedFieldIndexes.size() == 0) {
            return this.original;
        }

        // construct all static parts before encountering the first computed field
        for (int i = 0; i < computedFieldIndexes.get(0); i++) {
            root.append(prefixSegments.get(i)).append("/");
        }

        // remove last "/" and append it only if needed
        String finalRoot = root.toString();
        finalRoot = finalRoot.substring(0, finalRoot.length() - 1);
        return ExternalDataUtils.appendSlash(finalRoot, this.endsWithSlash);
    }

    public static class ComputedFieldDetails {
        private final List<List<String>> computedFieldNames;
        private final List<IAType> computedFieldTypes;
        private final List<Integer> computedFieldIndexes;
        private final Map<Integer, Pair<List<List<String>>, List<IAType>>> computedFields = new HashMap<>();
        private final ARecordType recordType;

        public ComputedFieldDetails(List<List<String>> computedFieldNames, List<IAType> computedFieldTypes,
                List<Integer> computedFieldIndexes) throws AlgebricksException {
            this.computedFieldNames = computedFieldNames;
            this.computedFieldTypes = computedFieldTypes;
            this.computedFieldIndexes = computedFieldIndexes;

            this.recordType = getRecordTypeWithFieldTypes(computedFieldNames, computedFieldTypes);

            for (int i = 0; i < computedFieldIndexes.size(); i++) {
                int index = computedFieldIndexes.get(i);

                if (computedFields.containsKey(index)) {
                    Pair<List<List<String>>, List<IAType>> pair = computedFields.get(index);
                    pair.getLeft().add(computedFieldNames.get(i));
                    pair.getRight().add(computedFieldTypes.get(i));
                } else {
                    List<List<String>> names = new ArrayList<>();
                    List<IAType> types = new ArrayList<>();

                    names.add(computedFieldNames.get(i));
                    types.add(computedFieldTypes.get(i));
                    computedFields.put(index, Pair.of(names, types));
                }
            }
        }

        public boolean isEmpty() {
            return computedFieldNames.isEmpty();
        }

        public List<List<String>> getComputedFieldNames() {
            return computedFieldNames;
        }

        public List<IAType> getComputedFieldTypes() {
            return computedFieldTypes;
        }

        public List<Integer> getComputedFieldIndexes() {
            return computedFieldIndexes;
        }

        public ARecordType getRecordType() {
            return recordType;
        }

        public Map<Integer, Pair<List<List<String>>, List<IAType>>> getComputedFields() {
            return computedFields;
        }

        @Override
        public String toString() {
            return computedFields.toString();
        }
    }
}
