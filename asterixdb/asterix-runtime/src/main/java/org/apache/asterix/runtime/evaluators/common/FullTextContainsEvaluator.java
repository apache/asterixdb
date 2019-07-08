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
package org.apache.asterix.runtime.evaluators.common;

import java.io.DataOutput;

import org.apache.asterix.formats.nontagged.BinaryComparatorFactoryProvider;
import org.apache.asterix.formats.nontagged.BinaryTokenizerFactoryProvider;
import org.apache.asterix.formats.nontagged.SerializerDeserializerProvider;
import org.apache.asterix.om.base.ABoolean;
import org.apache.asterix.om.base.ANull;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.EnumDeserializer;
import org.apache.asterix.om.types.hierachy.ATypeHierarchy;
import org.apache.asterix.runtime.evaluators.functions.FullTextContainsDescriptor;
import org.apache.asterix.runtime.evaluators.functions.PointableHelper;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.dataflow.value.IBinaryComparator;
import org.apache.hyracks.api.dataflow.value.IBinaryHashFunction;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.TaggedValuePointable;
import org.apache.hyracks.data.std.primitive.UTF8StringLowercaseTokenPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.data.std.util.BinaryEntry;
import org.apache.hyracks.data.std.util.BinaryHashSet;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.DelimitedUTF8StringBinaryTokenizer;
import org.apache.hyracks.storage.am.lsm.invertedindex.tokenizers.IBinaryTokenizer;
import org.apache.hyracks.util.string.UTF8StringUtil;

public class FullTextContainsEvaluator implements IScalarEvaluator {

    // assuming type indicator in serde format
    protected static final int TYPE_INDICATOR_SIZE = 1;

    protected final ArrayBackedValueStorage resultStorage = new ArrayBackedValueStorage();
    protected final DataOutput out = resultStorage.getDataOutput();
    protected final TaggedValuePointable argLeft = TaggedValuePointable.FACTORY.createPointable();
    protected final TaggedValuePointable argRight = TaggedValuePointable.FACTORY.createPointable();
    protected TaggedValuePointable[] argOptions;
    protected final IScalarEvaluator evalLeft;
    protected final IScalarEvaluator evalRight;
    protected IScalarEvaluator[] evalOptions;
    protected IPointable outLeft = VoidPointable.FACTORY.createPointable();
    protected IPointable outRight = VoidPointable.FACTORY.createPointable();
    protected IPointable[] outOptions;
    protected int optionArgsLength;

    // To conduct a full-text search, we convert all strings to the lower case.
    // In addition, since each token does not include the length information (2 bytes) in the beginning,
    // We need to have a different binary comparator that is different from a standard string comparator.
    // i.e. A token comparator that receives the length of a token as a parameter.
    private final IBinaryComparator strLowerCaseTokenCmp =
            BinaryComparatorFactoryProvider.UTF8STRING_LOWERCASE_TOKEN_POINTABLE_INSTANCE.createBinaryComparator();
    private final IBinaryComparator strLowerCaseCmp =
            BinaryComparatorFactoryProvider.UTF8STRING_LOWERCASE_POINTABLE_INSTANCE.createBinaryComparator();
    private IBinaryTokenizer tokenizerForLeftArray = null;
    private IBinaryTokenizer tokenizerForRightArray = null;

    // Case insensitive hash for full-text search
    private IBinaryHashFunction hashFunc = null;

    // keyEntry used in the hash-set
    private BinaryEntry keyEntry = null;

    // Parameter: number of bucket, frame size, hashFunction, Comparator, byte
    // array that contains the key
    private BinaryHashSet rightHashSet = null;

    // Keeps the query array. This is used to check whether the query predicate has been changed (e.g., join case)
    private byte[] queryArray = null;
    private int queryArrayStartOffset = -1;
    private int queryArrayLength = -1;

    // If the following is 1, then we will do a disjunctive search.
    // Else if it is equal to the number of tokens, then we will do a conjunctive search.
    private int occurrenceThreshold = 1;

    static final int HASH_SET_SLOT_SIZE = 101;
    static final int HASH_SET_FRAME_SIZE = 32768;

    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ABoolean> serde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ABOOLEAN);
    @SuppressWarnings("unchecked")
    protected ISerializerDeserializer<ANull> nullSerde =
            SerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ANULL);

    public FullTextContainsEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext context)
            throws HyracksDataException {
        evalLeft = args[0].createScalarEvaluator(context);
        evalRight = args[1].createScalarEvaluator(context);
        optionArgsLength = args.length - 2;
        this.evalOptions = new IScalarEvaluator[optionArgsLength];
        this.outOptions = new IPointable[optionArgsLength];
        this.argOptions = new TaggedValuePointable[optionArgsLength];
        // Full-text search options
        for (int i = 0; i < optionArgsLength; i++) {
            this.evalOptions[i] = args[i + 2].createScalarEvaluator(context);
            this.outOptions[i] = VoidPointable.FACTORY.createPointable();
            this.argOptions[i] = TaggedValuePointable.FACTORY.createPointable();
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        boolean isReturnNull = false;
        resultStorage.reset();

        evalLeft.evaluate(tuple, argLeft);
        evalRight.evaluate(tuple, argRight);

        if (PointableHelper.checkAndSetMissingOrNull(result, argLeft, argRight)) {
            if (result.getByteArray()[0] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                return;
            }

            // null value, but check other arguments for missing first (higher priority)
            isReturnNull = true;
        }

        argLeft.getValue(outLeft);
        argRight.getValue(outRight);

        for (int i = 0; i < optionArgsLength; i++) {
            evalOptions[i].evaluate(tuple, argOptions[i]);

            if (PointableHelper.checkAndSetMissingOrNull(result, argOptions[i])) {
                if (result.getByteArray()[0] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                    return;
                }

                // null value, but check other arguments for missing first (higher priority)
                isReturnNull = true;
            }

            argOptions[i].getValue(outOptions[i]);
        }

        if (isReturnNull) {
            PointableHelper.setNull(result);
            return;
        }

        ATypeTag typeTag1 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argLeft.getTag());
        ATypeTag typeTag2 = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argRight.getTag());

        // Checks whether two appropriate types are provided or not. If not, null will be written.
        if (!checkArgTypes(typeTag1, typeTag2)) {
            try {
                nullSerde.serialize(ANull.NULL, out);
            } catch (HyracksDataException e) {
                throw HyracksDataException.create(e);
            }
            result.set(resultStorage);
            return;
        }

        try {
            ABoolean b = fullTextContainsWithArg(typeTag2, argLeft, argRight) ? ABoolean.TRUE : ABoolean.FALSE;
            serde.serialize(b, out);
        } catch (HyracksDataException e1) {
            throw HyracksDataException.create(e1);
        }
        result.set(resultStorage);
    }

    /**
     * Conducts a full-text search. The basic logic is as follows.
     * 1) Tokenizes the given query predicate(s). Puts them into a hash set.
     * 2) Tokenizes the given field. For each token, checks whether the hash set contains it.
     * If so, increase foundCount for a newly found token.
     * 3) As soon as the foundCount becomes the given threshold, stops the search and returns true.
     * After traversing all tokens and still the foundCount is less than the given threshold, then returns false.
     */
    private boolean fullTextContainsWithArg(ATypeTag typeTag2, IPointable arg1, IPointable arg2)
            throws HyracksDataException {
        // Since a fulltext search form is "ftcontains(X,Y,options)",
        // X (document) is the left side and Y (query predicate) is the right side.

        // Initialize variables that are required to conduct full-text search. (e.g., hash-set, tokenizer ...)
        if (rightHashSet == null) {
            initializeFullTextContains();
        }

        // Type tag checking is already done in the previous steps.
        // So we directly conduct the full-text search process.
        // The right side contains the query predicates
        byte[] arg2Array = arg2.getByteArray();

        // Checks whether a new query predicate is introduced.
        // If not, we can re-use the query predicate array we have already created.
        if (!partOfArrayEquals(queryArray, queryArrayStartOffset, queryArrayLength, arg2Array, arg2.getStartOffset(),
                arg2.getLength())) {
            resetQueryArrayAndRight(arg2Array, typeTag2, arg2);
        } else {
            // The query predicate remains the same. However, the count of each token should be reset to zero.
            // Here, we visit all elements to clear the count.
            rightHashSet.clearFoundCount();
        }

        return readLeftAndConductSearch(arg1);
    }

    private void initializeFullTextContains() {
        // We use a hash set to store tokens from the right side (query predicate).
        // Initialize necessary variables.
        hashFunc = new PointableBinaryHashFunctionFactory(UTF8StringLowercaseTokenPointable.FACTORY)
                .createBinaryHashFunction();
        keyEntry = new BinaryEntry();
        // Parameter: number of bucket, frame size, hashFunction, Comparator, byte array
        // that contains the key (this array will be set later.)
        rightHashSet = new BinaryHashSet(HASH_SET_SLOT_SIZE, HASH_SET_FRAME_SIZE, hashFunc, strLowerCaseTokenCmp, null);
        tokenizerForLeftArray = BinaryTokenizerFactoryProvider.INSTANCE
                .getWordTokenizerFactory(ATypeTag.STRING, false, true).createTokenizer();
    }

    void resetQueryArrayAndRight(byte[] arg2Array, ATypeTag typeTag2, IPointable arg2) throws HyracksDataException {
        // If the right side is an (un)ordered list, we need to apply the (un)ordered list tokenizer.
        switch (typeTag2) {
            case ARRAY:
                tokenizerForRightArray = BinaryTokenizerFactoryProvider.INSTANCE
                        .getWordTokenizerFactory(ATypeTag.ARRAY, false, true).createTokenizer();
                break;
            case MULTISET:
                tokenizerForRightArray = BinaryTokenizerFactoryProvider.INSTANCE
                        .getWordTokenizerFactory(ATypeTag.MULTISET, false, true).createTokenizer();
                break;
            case STRING:
                tokenizerForRightArray = BinaryTokenizerFactoryProvider.INSTANCE
                        .getWordTokenizerFactory(ATypeTag.STRING, false, true).createTokenizer();
                break;
            default:
                break;
        }

        queryArray = arg2Array;
        queryArrayStartOffset = arg2.getStartOffset();
        queryArrayLength = arg2.getLength();

        // Clear hash set for the search predicates.
        rightHashSet.clear();
        rightHashSet.setRefArray(queryArray);

        // Token count in this query
        int queryTokenCount = 0;
        int uniqueQueryTokenCount = 0;

        int numBytesToStoreLength;

        // Reset the tokenizer for the given keywords in the given query
        if (typeTag2 == ATypeTag.STRING) {
            // How many bytes are required to store the length of the given token?
            numBytesToStoreLength = UTF8StringUtil
                    .getNumBytesToStoreLength(UTF8StringUtil.getUTFLength(queryArray, queryArrayStartOffset));
            queryArrayStartOffset = queryArrayStartOffset + numBytesToStoreLength;
            queryArrayLength = queryArrayLength - numBytesToStoreLength;
        }
        tokenizerForRightArray.reset(queryArray, queryArrayStartOffset, queryArrayLength);

        // Create tokens from the given query predicate
        while (tokenizerForRightArray.hasNext()) {
            tokenizerForRightArray.next();
            queryTokenCount++;

            // Insert the starting position and the length of the current token into the hash set.
            // We don't store the actual value of this token since we can access it via offset and length.
            int tokenOffset = tokenizerForRightArray.getToken().getStartOffset();
            int tokenLength = tokenizerForRightArray.getToken().getTokenLength();

            // If a token comes from a string tokenizer, each token doesn't have the length data
            // in the beginning. Instead, if a token comes from an (un)ordered list, each token has
            // the length data in the beginning. Since KeyEntry keeps the length data
            // as a parameter, we need to adjust token offset and length in this case.
            // e.g., 8database <--- we only need to store the offset of 'd' and length 8.
            if (typeTag2 == ATypeTag.ARRAY || typeTag2 == ATypeTag.MULTISET) {
                // How many bytes are required to store the length of the given token?
                numBytesToStoreLength = UTF8StringUtil.getNumBytesToStoreLength(
                        UTF8StringUtil.getUTFLength(tokenizerForRightArray.getToken().getData(),
                                tokenizerForRightArray.getToken().getStartOffset()));
                tokenOffset = tokenOffset + numBytesToStoreLength;
                tokenLength = tokenLength - numBytesToStoreLength;
            }
            keyEntry.set(tokenOffset, tokenLength);

            // Check whether the given token is a phrase.
            // Currently, for the full-text search, we don't support a phrase search yet.
            // So, each query predicate should have only one token.
            // The same logic should be applied in AbstractTOccurrenceSearcher() class.
            checkWhetherFullTextPredicateIsPhrase(typeTag2, queryArray, tokenOffset, tokenLength, queryTokenCount);

            // Count the number of tokens in the given query. We only count the unique tokens.
            // We only care about the first insertion of the token into the hash set
            // since we apply the set semantics.
            // e.g., if a query predicate is ["database","system","database"],
            // then "database" should be counted only once.
            // Thus, when we find the current token (we don't increase the count in this case),
            // it should not exist.
            if (rightHashSet.find(keyEntry, queryArray, false) == -1) {
                rightHashSet.put(keyEntry);
                uniqueQueryTokenCount++;
            }

        }

        // Apply the full-text search option here
        // Based on the search mode option - "any" or "all", set the occurrence threshold of tokens.
        setFullTextOption(argOptions, uniqueQueryTokenCount);
    }

    private void checkWhetherFullTextPredicateIsPhrase(ATypeTag typeTag, byte[] refArray, int tokenOffset,
            int tokenLength, int queryTokenCount) throws HyracksDataException {
        switch (typeTag) {
            case STRING:
                if (queryTokenCount > 1) {
                    throw new HyracksDataException(
                            "Phrase in Full-text search is not supported. An expression should include only one word.");
                }
                break;
            case ARRAY:
            case MULTISET:
                for (int j = 0; j < tokenLength; j++) {
                    if (DelimitedUTF8StringBinaryTokenizer.isSeparator((char) refArray[tokenOffset + j])) {
                        throw new HyracksDataException(
                                "Phrase in Full-text is not supported. An expression should include only one word."
                                        + (char) refArray[tokenOffset + j] + " " + refArray[tokenOffset + j]);
                    }
                }
                break;
            default:
                throw new HyracksDataException("Full-text search can be only executed on STRING or (UN)ORDERED LIST.");
        }
    }

    /**
     * Sets the full-text options. The odd element is an option name and the even element is the argument
     * for that option. (e.g., argOptions[0] = "mode", argOptions[1] = "all")
     */
    private void setFullTextOption(IPointable[] argOptions, int uniqueQueryTokenCount) throws HyracksDataException {
        // By default, we conduct a conjunctive search.
        occurrenceThreshold = uniqueQueryTokenCount;
        for (int i = 0; i < optionArgsLength; i = i + 2) {
            // mode option
            if (compareStrInByteArrayAndPointable(FullTextContainsDescriptor.getSearchModeOptionArray(), argOptions[i],
                    true) == 0) {
                if (compareStrInByteArrayAndPointable(FullTextContainsDescriptor.getDisjunctiveFTSearchOptionArray(),
                        argOptions[i + 1], true) == 0) {
                    // ANY
                    occurrenceThreshold = 1;
                } else if (compareStrInByteArrayAndPointable(
                        FullTextContainsDescriptor.getConjunctiveFTSearchOptionArray(), argOptions[i + 1], true) == 0) {
                    // ALL
                    occurrenceThreshold = uniqueQueryTokenCount;
                }
            }
        }
    }

    boolean readLeftAndConductSearch(IPointable arg1) throws HyracksDataException {
        // Now, we traverse the left side (document field) and tokenize the array and check whether each token
        // exists in the hash set. If it's the first time we find it, we increase foundCount.
        // As soon as foundCount is greater than occurrenceThreshold, we return true and stop.
        int foundCount = 0;

        // The left side: field (document)
        // Resets the tokenizer for the given keywords in a document.

        // How many bytes are required to store the length of the given string?
        int numBytesToStoreLength = UTF8StringUtil
                .getNumBytesToStoreLength(UTF8StringUtil.getUTFLength(arg1.getByteArray(), arg1.getStartOffset()));
        int startOffset = arg1.getStartOffset() + numBytesToStoreLength;
        int length = arg1.getLength() - numBytesToStoreLength;

        tokenizerForLeftArray.reset(arg1.getByteArray(), startOffset, length);

        // Creates tokens from a field in the left side (document)
        while (tokenizerForLeftArray.hasNext()) {
            tokenizerForLeftArray.next();

            // Records the starting position and the length of the current token.
            keyEntry.set(tokenizerForLeftArray.getToken().getStartOffset(),
                    tokenizerForLeftArray.getToken().getTokenLength());

            // Checks whether this token exists in the query hash-set.
            // We don't count multiple occurrence of a token now.
            // So, finding the same query predicate twice will not be counted as a found.
            if (rightHashSet.find(keyEntry, arg1.getByteArray(), true) == 1) {
                foundCount++;
                if (foundCount >= occurrenceThreshold) {
                    return true;
                }
            }
        }

        // Traversed all tokens. However, the count is not greater than the threshold.
        return false;
    }

    private int compareStrInByteArrayAndPointable(byte[] left, IPointable right, boolean rightTypeTagIncluded)
            throws HyracksDataException {
        int rightTypeTagLength = rightTypeTagIncluded ? 1 : 0;

        return strLowerCaseCmp.compare(left, 0, left.length, right.getByteArray(),
                right.getStartOffset() + rightTypeTagLength, right.getLength() - rightTypeTagLength);
    }

    /**
     * Checks the argument types. The argument1 should be a string.
     * The argument2 should be a string or an (un)ordered list.
     */
    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws HyracksDataException {
        if ((typeTag1 != ATypeTag.STRING) || (typeTag2 != ATypeTag.ARRAY && typeTag2 != ATypeTag.MULTISET
                && !ATypeHierarchy.isCompatible(typeTag1, typeTag2))) {
            return false;
        }
        return true;
    }

    /**
     * Checks whether the content of the given two arrays are equal.
     * The code is utilizing the Arrays.equals() code. The difference is that
     * this method only compares the certain portion of each array.
     */
    private static boolean partOfArrayEquals(byte[] array1, int start1, int length1, byte[] array2, int start2,
            int length2) {
        // Sanity check
        if (length1 != length2 || array1 == null || array2 == null) {
            return false;
        }

        if (array1 == array2 && start1 == start2 && length1 == length2) {
            return true;
        }

        int offset = 0;
        while (offset < length1) {
            if (array1[start1 + offset] != array2[start2 + offset]) {
                return false;
            }
            offset++;
        }

        return true;
    }

}
