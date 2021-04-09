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

package org.apache.asterix.common.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.exceptions.ErrorCode;

/**
 * This class represents a dataverse name.
 * The logical model is an ordered list of strings (name parts).
 * Use {@link #create(List)} to create a dataverse name from its parts
 * and {@link #getParts()} to obtain a list of parts from given dataverse name.
 * <p>
 * Each dataverse name can be encoded into a single string (called a canonical form) by
 * {@link #getCanonicalForm()} and decoded back from it with {@link #createFromCanonicalForm(String)}.
 * The canonical form encoding concatenates name parts together with {@link #CANONICAL_FORM_PART_SEPARATOR '/'}
 * character.
 * <p>
 * E.g. the canonical form for a dataverse name {@code ["a", "b", "c"]} is {@code "a/b/c"}
 * <p>
 * {@link #toString()} returns a display form which is suitable for error messages,
 * and is a valid SQL++ multi-part identifier parsable by {@code IParser#parseMultipartIdentifier()}
 * <p>
 * Notes:
 * <li>
 * <ul>
 * {@link #getCanonicalForm()} is faster than {@link #getParts()} because this class stores the canonical form,
 * so {@link #getCanonicalForm()} just returns it while {@link #getParts()} performs parsing and string construction
 * for each name part.
 * </ul>
 * <ul>
 * {@link #toString()} result is cached, subsequent invocations just return the cached value.
 * </ul>
 * </li>
 */
public final class DataverseName implements Serializable, Comparable<DataverseName> {

    private static final long serialVersionUID = 3L;

    public static final String CANONICAL_FORM_PART_SEPARATOR = "/";

    public static final char DISPLAY_FORM_SEPARATOR_CHAR = '.';

    private static final char DISPLAY_FORM_QUOTE_CHAR = '`';

    private static final char DISPLAY_FORM_ESCAPE_CHAR = '\\';

    private final String canonicalForm;

    private transient volatile String displayForm;

    private DataverseName(String canonicalForm) {
        this.canonicalForm = Objects.requireNonNull(canonicalForm);
    }

    /**
     * Returns a scalar encoding of this dataverse name.
     * The returned value can be used to reconstruct this name by calling {@link #createFromCanonicalForm(String)}.
     * <p>
     * Warning: changing the canonical form encoding will impact backwards compatibility because it's stored in the
     * metadata datasets and might be returned to users through public APIs.
     */
    public String getCanonicalForm() {
        return canonicalForm;
    }

    /**
     * Returns a new list containing dataverse name parts
     */
    public List<String> getParts() {
        List<String> parts = new ArrayList<>(4);
        getParts(parts);
        return parts;
    }

    /**
     * Appends dataverse name parts into a given output collection
     */
    public void getParts(Collection<? super String> outParts) {
        getPartsFromCanonicalFormImpl(canonicalForm, outParts);
    }

    /**
     * Appends dataverse name parts into a given output collection
     */
    public static void getPartsFromCanonicalForm(String canonicalForm, Collection<? super String> outParts)
            throws AsterixException {
        int partCount = getPartsFromCanonicalFormImpl(canonicalForm, outParts);
        if (partCount <= 0) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, canonicalForm);
        }
    }

    public int getPartCount() {
        return getPartCountFromCanonicalFormImpl(canonicalForm);
    }

    public static int getPartCountFromCanonicalForm(String canonicalForm) throws AsterixException {
        int partCount = getPartCountFromCanonicalFormImpl(canonicalForm);
        if (partCount <= 0) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, canonicalForm);
        }
        return partCount;
    }

    /**
     * Returns a display form which suitable for error messages, and is a valid SQL++ multi-part identifier.
     */
    @Override
    public String toString() {
        return getDisplayForm();
    }

    private String getDisplayForm() {
        String result = displayForm;
        if (result == null) {
            StringBuilder sb = new StringBuilder(canonicalForm.length() + 1);
            getDisplayForm(sb);
            displayForm = result = sb.toString();
        }
        return result;
    }

    public void getDisplayForm(StringBuilder out) {
        getDisplayFormFromCanonicalFormImpl(canonicalForm, out);
    }

    public static void getDisplayFormFromCanonicalForm(String canonicalForm, StringBuilder out)
            throws AsterixException {
        int partCount = getDisplayFormFromCanonicalFormImpl(canonicalForm, out);
        if (partCount <= 0) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, canonicalForm);
        }
    }

    private static int getPartsFromCanonicalFormImpl(String canonicalForm, Collection<? super String> outParts) {
        return decodeCanonicalForm(canonicalForm, DataverseName::addPartToCollection, outParts);
    }

    private static int getPartCountFromCanonicalFormImpl(String canonicalForm) {
        return decodeCanonicalForm(canonicalForm, null, null);
    }

    private static int getDisplayFormFromCanonicalFormImpl(String canonicalForm, StringBuilder out) {
        int partCount = decodeCanonicalForm(canonicalForm, DataverseName::addPartToDisplayForm, out);
        if (partCount > 0) {
            out.setLength(out.length() - 1); // remove last separator char
        }
        return partCount;
    }

    @Override
    public int hashCode() {
        return canonicalForm.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DataverseName)) {
            return false;
        }
        DataverseName that = (DataverseName) obj;
        return canonicalForm.equals(that.canonicalForm);
    }

    @Override
    public int compareTo(DataverseName that) {
        return canonicalForm.compareTo(that.canonicalForm);
    }

    /**
     * Creates a new dataverse name from a given list of name parts.
     * Equivalent to {@code create(parts, 0, parts.size())}.
     */
    public static DataverseName create(List<String> parts) throws AsterixException {
        return create(parts, 0, parts.size());
    }

    /**
     * Creates a new dataverse name from a given list of name parts.
     *
     * @param parts
     *            list of name parts
     * @param fromIndex
     *            index to start from
     * @param toIndex
     *            index to stop at (exclusive, value at that index is not used)
     */
    public static DataverseName create(List<String> parts, int fromIndex, int toIndex) throws AsterixException {
        int partCount = toIndex - fromIndex;
        return partCount == 1 ? createSinglePartName(parts.get(fromIndex))
                : createMultiPartName(parts, fromIndex, toIndex);
    }

    /**
     * Creates a new dataverse name from its scalar encoding (canonical form) returned by {@link #getCanonicalForm()}
     */
    public static DataverseName createFromCanonicalForm(String canonicalForm) throws AsterixException {
        int partCount = getPartCountFromCanonicalFormImpl(canonicalForm);
        if (partCount <= 0) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, canonicalForm);
        }
        return new DataverseName(canonicalForm);
    }

    /**
     * Creates a single-part dataverse name.
     * Equivalent to {@code create(Collections.singletonList(singlePart))}, but performs faster.
     */
    public static DataverseName createSinglePartName(String singlePart) throws AsterixException {
        String canonicalForm = encodeSinglePartNamePartIntoCanonicalForm(singlePart);
        return new DataverseName(canonicalForm);
    }

    /**
     * Creates a new dataverse name for a built-in dataverse.
     * Validates that the canonical form of the created dataverse name is the same as its given single name part.
     */
    public static DataverseName createBuiltinDataverseName(String singlePart) {
        try {
            return createSinglePartName(singlePart); // 1-part name
        } catch (AsterixException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    private static DataverseName createMultiPartName(List<String> parts, int fromIndex, int toIndex)
            throws AsterixException {
        String canonicalForm = encodeMultiPartNameIntoCanonicalForm(parts, fromIndex, toIndex);
        return new DataverseName(canonicalForm);
    }

    private static String encodeMultiPartNameIntoCanonicalForm(List<String> parts, int fromIndex, int toIndex)
            throws AsterixException {
        Objects.requireNonNull(parts);
        int partCount = toIndex - fromIndex;
        if (partCount <= 0) {
            throw new IllegalArgumentException(fromIndex + "," + toIndex);
        }
        int canonicalFormLength = (partCount - 1) * CANONICAL_FORM_PART_SEPARATOR.length();
        for (int i = fromIndex; i < toIndex; i++) {
            canonicalFormLength += parts.get(i).length();
        }
        String sep = "";
        StringBuilder sb = new StringBuilder(canonicalFormLength);
        for (int i = fromIndex; i < toIndex; i++) {
            sb.append(sep);
            encodePartIntoCanonicalForm(parts.get(i), sb);
            sep = CANONICAL_FORM_PART_SEPARATOR;
        }
        return sb.toString();
    }

    private static String encodeSinglePartNamePartIntoCanonicalForm(String singlePart) throws AsterixException {
        validatePart(singlePart);
        return singlePart;
    }

    private static void encodePartIntoCanonicalForm(String part, StringBuilder out) throws AsterixException {
        validatePart(part);
        out.append(part);
    }

    private static void validatePart(String part) throws AsterixException {
        if (part.isEmpty() || part.contains(CANONICAL_FORM_PART_SEPARATOR)) {
            throw new AsterixException(ErrorCode.INVALID_DATABASE_OBJECT_NAME, part);
        }
    }

    private static <T> int decodeCanonicalForm(String canonicalForm, DataverseNamePartConsumer<T> partConsumer,
            T partConsumerArg) {
        int partCount = 0, from = 0, to;
        while ((to = canonicalForm.indexOf(CANONICAL_FORM_PART_SEPARATOR, from)) > from) {
            if (partConsumer != null) {
                partConsumer.accept(canonicalForm, from, to, partConsumerArg);
            }
            partCount++;
            from = to + CANONICAL_FORM_PART_SEPARATOR.length();
        }
        boolean hasOneMore = from != to && from < canonicalForm.length();
        if (!hasOneMore) {
            return 0;
        }
        if (partConsumer != null) {
            partConsumer.accept(canonicalForm, from, canonicalForm.length(), partConsumerArg);
        }
        partCount++;
        return partCount;
    }

    @FunctionalInterface
    private interface DataverseNamePartConsumer<T> {
        void accept(String canonicalForm, int from, int to, T arg);
    }

    private static void addPartToCollection(String canonicalForm, int from, int to, Collection<? super String> out) {
        out.add(canonicalForm.substring(from, to));
    }

    private static void addPartToDisplayForm(String canonicalForm, int from, int to, StringBuilder out) {
        boolean needQuote = false;
        for (int i = from; i < to; i++) {
            char c = canonicalForm.charAt(i);
            boolean noQuote = isLetter(c) || c == '_' || (i > 0 && (isDigit(c) || c == '$'));
            if (!noQuote) {
                needQuote = true;
                break;
            }
        }
        if (needQuote) {
            out.append(DISPLAY_FORM_QUOTE_CHAR);
        }
        for (int i = from; i < to; i++) {
            char c = canonicalForm.charAt(i);
            if (c == DISPLAY_FORM_ESCAPE_CHAR || c == DISPLAY_FORM_QUOTE_CHAR) {
                out.append(DISPLAY_FORM_ESCAPE_CHAR);
            }
            out.append(c);
        }
        if (needQuote) {
            out.append(DISPLAY_FORM_QUOTE_CHAR);
        }
        out.append(DISPLAY_FORM_SEPARATOR_CHAR);
    }

    private static boolean isLetter(char c) {
        return ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z');
    }

    private static boolean isDigit(char c) {
        return '0' <= c && c <= '9';
    }
}
