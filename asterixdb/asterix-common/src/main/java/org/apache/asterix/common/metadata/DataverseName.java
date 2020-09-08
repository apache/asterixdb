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
import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;

/**
 * This class represents a dataverse name.
 * The logical model is an ordered list of strings (name parts).
 * Use {@link #create(List)} to create a dataverse name from its parts
 * and {@link #getParts()} to obtain a list of parts from given dataverse name.
 * <p>
 * Each dataverse name can be encoded into a single string (called a canonical form) by
 * {@link #getCanonicalForm()} and decoded back from it with {@link #createFromCanonicalForm(String)}.
 * The canonical form encoding concatenates name parts together with {@link #CANONICAL_FORM_SEPARATOR_CHAR '.'}
 * character. The {@link #CANONICAL_FORM_ESCAPE_CHAR '@'} character is used to escape
 * {@link #CANONICAL_FORM_SEPARATOR_CHAR '.'} and itself in each name part prior to concatenation.
 * <p>
 * E.g. the canonical form for a dataverse name {@code ["a", "b", "c"]} is {@code "a.b.c"}
 * <p>
 * {@link #toString()} returns a display form which is a {@link #CANONICAL_FORM_SEPARATOR_CHAR '.'} separated
 * concatenation of name parts without escaping. In general it's impossible to reconstruct a dataverse name from
 * its display form.
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

    private static final long serialVersionUID = 1L;

    public static final char CANONICAL_FORM_SEPARATOR_CHAR = '.';

    private static final char CANONICAL_FORM_ESCAPE_CHAR = '@';

    public static final char DISPLAY_FORM_SEPARATOR_CHAR = '.';

    private static final char DISPLAY_FORM_QUOTE_CHAR = '`';

    private static final char DISPLAY_FORM_ESCAPE_CHAR = '\\';

    private static final char[] CANONICAL_FORM_SEPARATOR_AND_ESCAPE_CHARS =
            new char[] { CANONICAL_FORM_SEPARATOR_CHAR, CANONICAL_FORM_ESCAPE_CHAR };

    private final boolean isMultiPart;

    private final String canonicalForm;

    private transient volatile String displayForm;

    private DataverseName(String canonicalForm, boolean isMultiPart) {
        this.canonicalForm = Objects.requireNonNull(canonicalForm);
        this.isMultiPart = isMultiPart;
    }

    /**
     * Returns whether this dataverse name contains multiple name parts or not.
     */
    public boolean isMultiPart() {
        return isMultiPart;
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
        List<String> parts = new ArrayList<>(isMultiPart ? 4 : 1);
        getParts(parts);
        return parts;
    }

    /**
     * Appends dataverse name parts into a given output collection
     */
    public void getParts(Collection<? super String> outParts) {
        getPartsFromCanonicalForm(canonicalForm, isMultiPart, outParts);
    }

    /**
     * Appends dataverse name parts into a given output collection
     */
    public static void getPartsFromCanonicalForm(String canonicalForm, Collection<? super String> outParts) {
        getPartsFromCanonicalForm(canonicalForm, isMultiPartCanonicalForm(canonicalForm), outParts);
    }

    /**
     * Appends dataverse name parts into a given output collection
     */
    private static void getPartsFromCanonicalForm(String canonicalForm, boolean isMultiPart,
            Collection<? super String> outParts) {
        if (isMultiPart) {
            decodeCanonicalForm(canonicalForm, DataverseName::addPartToCollection, outParts);
        } else {
            outParts.add(decodeSinglePartNameFromCanonicalForm(canonicalForm));
        }
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
        getDisplayFormFromCanonicalForm(canonicalForm, isMultiPart, out);
    }

    public static void getDisplayFormFromCanonicalForm(String canonicalForm, StringBuilder out) {
        getDisplayFormFromCanonicalForm(canonicalForm, isMultiPartCanonicalForm(canonicalForm), out);
    }

    private static void getDisplayFormFromCanonicalForm(String canonicalForm, boolean isMultiPart, StringBuilder out) {
        if (isMultiPart) {
            decodeCanonicalForm(canonicalForm, DataverseName::addPartToDisplayForm, out);
        } else {
            String singlePart = decodeSinglePartNameFromCanonicalForm(canonicalForm);
            addPartToDisplayForm(singlePart, out);
        }
        out.setLength(out.length() - 1); // remove last separator char
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
    public static DataverseName create(List<String> parts) {
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
    public static DataverseName create(List<String> parts, int fromIndex, int toIndex) {
        int partCount = toIndex - fromIndex;
        return partCount == 1 ? createSinglePartName(parts.get(fromIndex))
                : createMultiPartName(parts, fromIndex, toIndex);
    }

    /**
     * Creates a new dataverse name from its scalar encoding (canonical form) returned by {@link #getCanonicalForm()}
     */
    public static DataverseName createFromCanonicalForm(String canonicalForm) {
        boolean isMultiPart = isMultiPartCanonicalForm(canonicalForm);
        return new DataverseName(canonicalForm, isMultiPart);
    }

    /**
     * Creates a single-part dataverse name.
     * Equivalent to {@code create(Collections.singletonList(singlePart))}, but performs faster.
     */
    public static DataverseName createSinglePartName(String singlePart) {
        String canonicalForm = encodeSinglePartNamePartIntoCanonicalForm(singlePart);
        return new DataverseName(canonicalForm, false);
    }

    /**
     * Creates a new dataverse name for a built-in dataverse.
     * Validates that the canonical form of the created dataverse name is the same as its given single name part.
     */
    public static DataverseName createBuiltinDataverseName(String singlePart) {
        if (StringUtils.containsAny(singlePart, CANONICAL_FORM_SEPARATOR_AND_ESCAPE_CHARS)) {
            throw new IllegalArgumentException(singlePart);
        }
        DataverseName dataverseName = createSinglePartName(singlePart); // 1-part name
        String canonicalForm = dataverseName.getCanonicalForm();
        if (!canonicalForm.equals(singlePart)) {
            throw new IllegalStateException(canonicalForm + "!=" + singlePart);
        }
        return dataverseName;
    }

    private static DataverseName createMultiPartName(List<String> parts, int fromIndex, int toIndex) {
        String canonicalForm = encodeMultiPartNameIntoCanonicalForm(parts, fromIndex, toIndex);
        return new DataverseName(canonicalForm, true);
    }

    private static String encodeMultiPartNameIntoCanonicalForm(List<String> parts, int fromIndex, int toIndex) {
        Objects.requireNonNull(parts);
        int partCount = toIndex - fromIndex;
        if (partCount <= 0) {
            throw new IllegalArgumentException(fromIndex + "," + toIndex);
        }
        StringBuilder sb = new StringBuilder(32);
        for (int i = 0; i < partCount; i++) {
            if (i > 0) {
                sb.append(CANONICAL_FORM_SEPARATOR_CHAR);
            }
            encodePartIntoCanonicalForm(parts.get(fromIndex + i), sb);
        }
        return sb.toString();
    }

    private static String encodeSinglePartNamePartIntoCanonicalForm(String singlePart) {
        if (StringUtils.indexOfAny(singlePart, CANONICAL_FORM_SEPARATOR_AND_ESCAPE_CHARS) < 0) {
            // no escaping needed
            return singlePart;
        }
        StringBuilder sb = new StringBuilder(singlePart.length() + 4);
        encodePartIntoCanonicalForm(singlePart, sb);
        return sb.toString();
    }

    private static void encodePartIntoCanonicalForm(String part, StringBuilder out) {
        for (int i = 0, ln = part.length(); i < ln; i++) {
            char c = part.charAt(i);
            if (c == CANONICAL_FORM_SEPARATOR_CHAR || c == CANONICAL_FORM_ESCAPE_CHAR) {
                out.append(CANONICAL_FORM_ESCAPE_CHAR);
            }
            out.append(c);
        }
    }

    private static <T> void decodeCanonicalForm(String canonicalForm, BiConsumer<CharSequence, T> partConsumer,
            T partConsumerArg) {
        int ln = canonicalForm.length();
        StringBuilder sb = new StringBuilder(ln);
        for (int i = 0; i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case CANONICAL_FORM_SEPARATOR_CHAR:
                    partConsumer.accept(sb, partConsumerArg);
                    sb.setLength(0);
                    break;
                case CANONICAL_FORM_ESCAPE_CHAR:
                    i++;
                    c = canonicalForm.charAt(i);
                    // fall through to 'default'
                default:
                    sb.append(c);
                    break;
            }
        }
        if (sb.length() > 0) {
            partConsumer.accept(sb, partConsumerArg);
        }
    }

    // optimization for a single part name
    private static String decodeSinglePartNameFromCanonicalForm(String canonicalForm) {
        if (canonicalForm.indexOf(CANONICAL_FORM_ESCAPE_CHAR) < 0) {
            // no escaping was done
            return canonicalForm;
        }

        StringBuilder singlePart = new StringBuilder(canonicalForm.length());
        for (int i = 0, ln = canonicalForm.length(); i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case CANONICAL_FORM_SEPARATOR_CHAR:
                    throw new IllegalStateException(canonicalForm); // should never happen
                case CANONICAL_FORM_ESCAPE_CHAR:
                    i++;
                    c = canonicalForm.charAt(i);
                    // fall through to 'default'
                default:
                    singlePart.append(c);
                    break;
            }
        }
        return singlePart.toString();
    }

    private static boolean isMultiPartCanonicalForm(String canonicalForm) {
        for (int i = 0, ln = canonicalForm.length(); i < ln; i++) {
            char c = canonicalForm.charAt(i);
            switch (c) {
                case CANONICAL_FORM_SEPARATOR_CHAR:
                    return true;
                case CANONICAL_FORM_ESCAPE_CHAR:
                    i++;
                    break;
            }
        }
        return false;
    }

    private static void addPartToCollection(CharSequence part, Collection<? super String> out) {
        out.add(part.toString());
    }

    private static void addPartToDisplayForm(CharSequence part, StringBuilder out) {
        boolean needQuote = false;
        for (int i = 0, ln = part.length(); i < ln; i++) {
            char c = part.charAt(i);
            boolean noQuote = isLetter(c) || c == '_' || (i > 0 && (isDigit(c) || c == '$'));
            if (!noQuote) {
                needQuote = true;
                break;
            }
        }
        if (needQuote) {
            out.append(DISPLAY_FORM_QUOTE_CHAR);
        }
        for (int i = 0, ln = part.length(); i < ln; i++) {
            char c = part.charAt(i);
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