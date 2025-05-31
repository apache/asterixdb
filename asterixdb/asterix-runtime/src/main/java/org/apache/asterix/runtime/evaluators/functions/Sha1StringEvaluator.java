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

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.asterix.dataflow.data.nontagged.printers.PrintTools;
import org.apache.asterix.om.exceptions.ExceptionUtil;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluator;
import org.apache.hyracks.algebricks.runtime.base.IScalarEvaluatorFactory;
import org.apache.hyracks.api.context.IEvaluatorContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.api.exceptions.SourceLocation;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.primitive.VoidPointable;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.apache.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import org.apache.hyracks.util.bytes.Base64Printer;
import org.apache.hyracks.util.bytes.HexPrinter;
import org.apache.hyracks.util.string.UTF8StringWriter;

public class Sha1StringEvaluator extends AbstractScalarEval {

    private final IScalarEvaluator eval;
    private final ArrayBackedValueStorage storage;
    private final IPointable argPtr;
    private final UTF8StringWriter writer;
    private final StringBuilder strBuilder;
    private final MessageDigest md;
    private final IEvaluatorContext ctx;
    private final Encoding encoding;

    Sha1StringEvaluator(IScalarEvaluatorFactory[] args, IEvaluatorContext ctx, SourceLocation srcLoc,
            FunctionIdentifier funId, Encoding encoding) throws HyracksDataException {
        super(srcLoc, funId);
        this.ctx = ctx;
        this.encoding = encoding;
        storage = new ArrayBackedValueStorage(43);
        eval = args[0].createScalarEvaluator(ctx);
        strBuilder = new StringBuilder(40);
        writer = new UTF8StringWriter();
        argPtr = new VoidPointable();
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public void evaluate(IFrameTupleReference tuple, IPointable result) throws HyracksDataException {
        eval.evaluate(tuple, argPtr);
        if (PointableHelper.checkAndSetMissingOrNull(result, argPtr)) {
            return;
        }
        byte[] bytes = argPtr.getByteArray();
        int offset = argPtr.getStartOffset();
        if (bytes[offset] != ATypeTag.SERIALIZED_STRING_TYPE_TAG) {
            PointableHelper.setNull(result);
            ExceptionUtil.warnTypeMismatch(ctx, srcLoc, funID, bytes[offset], 0, ATypeTag.STRING);
            return;
        }

        storage.reset();
        try {
            // convert modified UTF-8 bytes to UTF-8 bytes
            PrintTools.writeUTF8StringRaw(bytes, offset + 1, argPtr.getLength() - 1, storage.getDataOutput());
            md.update(storage.getByteArray(), storage.getStartOffset(), storage.getLength());
            byte[] digestBytes = md.digest();
            strBuilder.setLength(0);
            // convert UTF-8 bytes to hex or base64 string
            if (encoding == Encoding.HEX) {
                HexPrinter.printHexString(digestBytes, 0, digestBytes.length, strBuilder);
            } else {
                Base64Printer.printBase64Binary(digestBytes, 0, digestBytes.length, strBuilder);
            }
            // serialize the encoded string as modified UTF-8
            storage.reset();
            storage.getDataOutput().writeByte(ATypeTag.SERIALIZED_STRING_TYPE_TAG);
            writer.writeUTF8(strBuilder, storage.getDataOutput());
        } catch (IOException e) {
            throw HyracksDataException.create(e);
        }
        result.set(storage);
    }

    enum Encoding {
        HEX,
        BASE64;
    }
}
