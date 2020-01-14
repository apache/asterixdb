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
package org.apache.asterix.runtime.evaluators.functions;

import static org.apache.asterix.om.types.EnumDeserializer.ATYPETAGDESERIALIZER;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.asterix.common.annotations.MissingNullInOutFunction;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.runtime.evaluators.common.ListAccessor;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IEvaluatorContext;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.string.UTF8StringUtil;

/**
 * This evaluator performs string concatenation. It allows concatenating with or without a separator between each
 * concatenation token. It also can be controlled to allow strings and lists of strings, or list of strings only as
 * an input, based on the provided arguments.
 *
 * This evaluator is used by multiple functions that allow different arguments setup. This information is passed
 * to the evaluator using the {@code isAcceptListOnly} and {@code separatorPosition} arguments.
 */
@MissingNullInOutFunction
public abstract class AbstractConcatStringEval extends AbstractScalarEval {

    // Different functions provide different argument position for the separator, this value indicates at which
    // position the separator resides, if NO_SEPARATOR_POSITION value is provided, it means this function does not use
    // a separator
    private final int separatorPosition;
    static final int NO_SEPARATOR_POSITION = -1;

    // Context
    private final IEvaluatorContext ctx;

    // Storage
    final ArrayBackedValueStorage storage = new ArrayBackedValueStorage();
    final DataOutput storageDataOutput = storage.getDataOutput();

    // Evaluators
    private final IScalarEvaluator[] evals;
    private final IPointable[] pointables;
    protected ListAccessor listAccessor = new ListAccessor();

    private final byte[] tempLengthArray = new byte[5];

    // For handling missing, null, invalid data and warnings
    protected boolean isMissingMet = false;
    protected boolean isReturnNull = false;
    protected int argIndex = -1;
    protected byte[] expectedType = null;
    protected ATypeTag unsupportedType = null;

    protected final byte[] STRING_TYPE = new byte[] { ATypeTag.SERIALIZED_STRING_TYPE_TAG };

    public AbstractConcatStringEval(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx,
            SourceLocation sourceLocation, FunctionIdentifier functionIdentifier, int separatorPosition)
            throws HyracksDataException {
        super(sourceLocation, functionIdentifier);
        this.ctx = ctx;
        this.separatorPosition = separatorPosition;

        // Evaluators and Pointables
        pointables = new IPointable[args.length];
        evals = new IScalarEvaluator[args.length];
        for (int i = 0; i < args.length; i++) {
            evals[i] = args[i].createScalarEvaluator(ctx);
            pointables[i] = new VoidPointable();
        }
    }

    protected abstract boolean isAcceptedType(ATypeTag typeTag);

    protected abstract byte[] getExpectedType();

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        // Reset the members
        resetValidationVariables();

        // Evaluate and check the arguments (Missing/Null checks)
        for (int i = 0; i < evals.length; i++) {
            evals[i].evaluate(tuple, pointables[i]);
            isMissingMet = doMissingNullCheck(result, pointables[i]);
            if (isMissingMet) {
                return;
            }
        }

        // Terminate execution if any null outer argument has been encountered
        if (isReturnNull) {
            PointableHelper.setNull(result);
            return;
        }

        byte[] separatorBytes = null;
        int separatorStartOffset = 0;

        // Validate the separator type (if it is present)
        if (separatorPosition != NO_SEPARATOR_POSITION) {
            separatorBytes = pointables[separatorPosition].getByteArray();
            separatorStartOffset = pointables[separatorPosition].getStartOffset();
            ATypeTag separatorTypeTag = ATYPETAGDESERIALIZER.deserialize(separatorBytes[separatorStartOffset]);

            if (separatorTypeTag != ATypeTag.STRING) {
                isReturnNull = true;
                argIndex = separatorPosition;
                expectedType = STRING_TYPE;
                unsupportedType = separatorTypeTag;
            }
        }

        // Rest of arguments check
        for (int i = 0; i < pointables.length; i++) {
            // Separator can come at different positions, so when validating other arguments, skip separator position
            if (i != separatorPosition) {
                byte[] bytes = pointables[i].getByteArray();
                int startOffset = pointables[i].getStartOffset();
                ATypeTag typeTag = ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);

                if (!isAcceptedType(typeTag)) {
                    isReturnNull = true;
                    if (unsupportedType == null) {
                        argIndex = i;
                        expectedType = getExpectedType();
                        unsupportedType = typeTag;
                    }
                }

                // If the item is a list, we are checking list elements here
                if (typeTag.isListType()) {
                    listAccessor.reset(bytes, startOffset);

                    for (int j = 0; j < listAccessor.size(); j++) {
                        int itemStartOffset = listAccessor.getItemOffset(j);
                        ATypeTag itemTypeTag = listAccessor.getItemType(itemStartOffset);

                        if (itemTypeTag != ATypeTag.STRING) {
                            if (itemTypeTag == ATypeTag.MISSING) {
                                PointableHelper.setMissing(result);
                                return;
                            }

                            isReturnNull = true;
                            if (unsupportedType == null || itemTypeTag == ATypeTag.NULL) {
                                argIndex = getActualArgumentIndex(i, j);
                                expectedType = getExpectedType();
                                unsupportedType = itemTypeTag;
                            }
                        }
                    }
                }
            }
        }

        if (isReturnNull) {
            PointableHelper.setNull(result);
            if (unsupportedType != null && unsupportedType != ATypeTag.NULL) {
                ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, unsupportedType.serialize(), argIndex, expectedType);
            }
            return;
        }

        // Concatenate the strings
        int utf8Length = calculateStringLength(separatorBytes, separatorStartOffset);
        constructConcatenatedString(utf8Length, separatorBytes, separatorStartOffset);

        result.set(storage);
    }

    /**
     * Resets the validation variables to their default values at the start of each tuple run.
     */
    private void resetValidationVariables() {
        isMissingMet = false;
        isReturnNull = false;
        argIndex = -1;
        expectedType = null;
        unsupportedType = null;
    }

    /**
     * Performs the missing/null check.
     *
     * @param result result pointable
     * @param pointable pointable to be checked
     *
     * @return {@code true} if execution should stop (missing encountered), {@code false} otherwise.
     * @throws HyracksDataException Hyracks data exception
     */
    protected boolean doMissingNullCheck(IPointable result, IPointable pointable) throws HyracksDataException {
        if (PointableHelper.checkAndSetMissingOrNull(result, pointable)) {
            if (result.getByteArray()[result.getStartOffset()] == ATypeTag.SERIALIZED_MISSING_TYPE_TAG) {
                return true;
            }

            // null value, but check other arguments for missing first (higher priority)
            isReturnNull = true;
        }
        return false;
    }

    /**
     * One of the functions using this evaluator has its arguments (multiple arguments) internally converted into a
     * single argument as a list of arguments. This method can be overridden to use the position in the list to return
     * the correct position of the argument when reporting back.
     */
    int getActualArgumentIndex(int outArgumentIndex, int innerArgumentIndex) {
        return outArgumentIndex;
    }

    /**
     * Calculates the required total length for the generated string result
     *
     * @return The total string length
     */
    private int calculateStringLength(final byte[] separatorBytes, final int separatorStartOffset)
            throws HyracksDataException {
        int utf8Length = 0;

        // Separator length is fixed, calculate it once only (if present)
        int separatorLength = 0;
        if (separatorPosition != NO_SEPARATOR_POSITION) {
            separatorLength = UTF8StringUtil.getUTFLength(separatorBytes, separatorStartOffset + 1);
        }

        for (int i = 0; i < pointables.length; i++) {
            // Skip separator position
            if (i != separatorPosition) {
                byte[] bytes = pointables[i].getByteArray();
                int startOffset = pointables[i].getStartOffset();
                ATypeTag typeTag = ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);

                // Second argument is a string or array
                // Calculate total string length (string has variable length)
                try {
                    if (typeTag == ATypeTag.STRING) {
                        startOffset++; // skip the type tag byte
                        utf8Length += UTF8StringUtil.getUTFLength(bytes, startOffset);
                    } else {
                        listAccessor.reset(bytes, startOffset);

                        for (int j = 0; j < listAccessor.size(); j++) {
                            int itemStartOffset = listAccessor.getItemOffset(j);

                            if (listAccessor.itemsAreSelfDescribing()) {
                                itemStartOffset++;
                            }

                            utf8Length += UTF8StringUtil.getUTFLength(bytes, itemStartOffset);

                            // Ensure separator is present before applying the separator calculation
                            // Separator length (on last item, do not add separator length)
                            if (separatorPosition != NO_SEPARATOR_POSITION && j < listAccessor.size() - 1) {
                                utf8Length += separatorLength;
                            }
                        }
                    }
                } catch (IOException ex) {
                    throw HyracksDataException.create(ex);
                }

                // Ensure separator is present before applying the separator calculation
                // Separator length (on last item, do not add separator length)
                // Depending on separator position (start or end), we need to adjust the calculation
                if (separatorPosition != NO_SEPARATOR_POSITION
                        && i < pointables.length - (separatorPosition > 0 ? 2 : 1)) {
                    utf8Length += separatorLength;
                }
            }
        }

        return utf8Length;
    }

    /**
     * Constructs the concatenated string from the provided strings
     */
    private void constructConcatenatedString(final int utf8Length, final byte[] separatorBytes,
            final int separatorStartOffset) throws HyracksDataException {
        // Arguments validation is done in the length calculation step
        try {
            Arrays.fill(tempLengthArray, (byte) 0);
            int cbytes = UTF8StringUtil.encodeUTF8Length(utf8Length, tempLengthArray, 0);
            storage.reset();
            storageDataOutput.writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            storageDataOutput.write(tempLengthArray, 0, cbytes);

            // Separator length is fixed, calculate it once only (if present)
            int separatorLength = 0;
            int separatorMetaLength = 0;
            if (separatorPosition != NO_SEPARATOR_POSITION) {
                separatorLength = UTF8StringUtil.getUTFLength(separatorBytes, separatorStartOffset + 1);
                separatorMetaLength = UTF8StringUtil.getNumBytesToStoreLength(separatorLength);
            }

            int StringLength;
            for (int i = 0; i < pointables.length; i++) {
                // Skip separator position
                if (i != separatorPosition) {
                    byte[] bytes = pointables[i].getByteArray();
                    int startOffset = pointables[i].getStartOffset();
                    ATypeTag typeTag = ATYPETAGDESERIALIZER.deserialize(bytes[startOffset]);

                    if (typeTag == ATypeTag.STRING) {
                        startOffset++; // skip the type tag byte
                        StringLength = UTF8StringUtil.getUTFLength(bytes, startOffset);
                        storageDataOutput.write(bytes,
                                UTF8StringUtil.getNumBytesToStoreLength(StringLength) + startOffset, StringLength);
                    } else {
                        listAccessor.reset(bytes, startOffset);

                        for (int j = 0; j < listAccessor.size(); j++) {
                            int itemStartOffset = listAccessor.getItemOffset(j);

                            if (listAccessor.itemsAreSelfDescribing()) {
                                itemStartOffset++;
                            }

                            StringLength = UTF8StringUtil.getUTFLength(bytes, itemStartOffset);
                            storageDataOutput.write(bytes,
                                    UTF8StringUtil.getNumBytesToStoreLength(StringLength) + itemStartOffset,
                                    StringLength);

                            // Ensure separator is present before applying the separator calculation
                            // More arguments, add a separator
                            if (separatorPosition != NO_SEPARATOR_POSITION && j < listAccessor.size() - 1) {
                                storageDataOutput.write(separatorBytes, separatorMetaLength + separatorStartOffset + 1,
                                        separatorLength);
                            }
                        }
                    }

                    // Ensure separator is present before applying the separator calculation
                    // More arguments, add a separator
                    // Depending on separator position (start or end), we need to adjust the calculation
                    if (separatorPosition != NO_SEPARATOR_POSITION
                            && i < pointables.length - (separatorPosition > 0 ? 2 : 1)) {
                        storageDataOutput.write(separatorBytes, separatorMetaLength + separatorStartOffset + 1,
                                separatorLength);
                    }
                }
            }
        } catch (IOException ex) {
            throw HyracksDataException.create(ex);
        }
    }
}
