/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.asterix.runtime.evaluators.functions.binary;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.om.functions.AsterixBuiltinFunctions;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.asterix.runtime.evaluators.common.AsterixListAccessor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

import java.io.IOException;

public class BinaryConcatDescriptor extends AbstractScalarFunctionDynamicDescriptor {
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        @Override public IFunctionDescriptor createFunctionDescriptor() {
            return new BinaryConcatDescriptor();
        }
    };

    @Override public FunctionIdentifier getIdentifier() {
        return AsterixBuiltinFunctions.BINARY_CONCAT;
    }

    @Override public ICopyEvaluatorFactory createEvaluatorFactory(final ICopyEvaluatorFactory[] args)
            throws AlgebricksException {
        return new ICopyEvaluatorFactory() {
            @Override public ICopyEvaluator createEvaluator(final IDataOutputProvider output)
                    throws AlgebricksException {
                return new AbstractCopyEvaluator(output, args) {

                    private final AsterixListAccessor listAccessor = new AsterixListAccessor();
                    private final byte SER_BINARY_TYPE = ATypeTag.BINARY.serialize();

                    @Override public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                        ATypeTag typeTag = evaluateTuple(tuple, 0);
                        if (typeTag != ATypeTag.UNORDEREDLIST && typeTag != ATypeTag.ORDEREDLIST) {
                            throw new AlgebricksException(getIdentifier().getName()
                                    + ": expects input type ORDEREDLIST/UNORDEREDLIST, but got "
                                    + typeTag);
                        }
                        try {
                            listAccessor.reset(storages[0].getByteArray(), 0);

                            int concatLength = 0;
                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                ATypeTag itemType = listAccessor.getItemType(itemOffset);
                                if (itemType != ATypeTag.BINARY) {
                                    if (serializeNullIfAnyNull(itemType)) {
                                        return;
                                    }
                                    throw new AlgebricksException(getIdentifier().getName()
                                            + ": expects type STRING/NULL for the list item but got " + itemType);
                                }
                                concatLength += ByteArrayPointable.getLength(storages[0].getByteArray(), itemOffset);
                            }
                            if (concatLength > ByteArrayPointable.MAX_LENGTH) {
                                throw new AlgebricksException("the concatenated binary is too long.");
                            }
                            dataOutput.writeByte(SER_BINARY_TYPE);
                            dataOutput.writeShort(concatLength);

                            for (int i = 0; i < listAccessor.size(); i++) {
                                int itemOffset = listAccessor.getItemOffset(i);
                                int length = ByteArrayPointable.getLength(storages[0].getByteArray(), itemOffset);
                                dataOutput.write(storages[0].getByteArray(),
                                        itemOffset + ByteArrayPointable.SIZE_OF_LENGTH, length);
                            }
                        } catch (HyracksDataException e) {
                            throw new AlgebricksException(e);
                        } catch (IOException e) {
                            throw new AlgebricksException(e);
                        } catch (AsterixException e) {
                            throw new AlgebricksException(e);
                        }

                    }
                };

            }
        };

    }
}
