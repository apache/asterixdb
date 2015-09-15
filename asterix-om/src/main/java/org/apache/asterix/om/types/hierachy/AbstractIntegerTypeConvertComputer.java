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
package org.apache.asterix.om.types.hierachy;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.asterix.om.types.ATypeTag;

public abstract class AbstractIntegerTypeConvertComputer implements ITypeConvertComputer {

    // Refer to the following to convert byte array to integer types, and vice versa.
    // https://docs.oracle.com/javase/7/docs/api/java/io/DataOutput.html
    // https://docs.oracle.com/javase/7/docs/api/java/io/DataInput.html
    public void convertIntegerType(byte[] data, int start, int length, DataOutput out, ATypeTag targetType,
            int targetTypeLength) throws IOException {
        long num = 0;
        //        for (int i = start; i < start + length; i++) {
        //            num += (data[i] & 0xff) << ((length - 1 - (i - start)) * 8);
        //        }

        // Read source values
        switch (length) {
            case 1:
                // INT8
                num = (data[start] & 0xff);
                break;

            case 2:
                // INT16
                num = (short) ((data[start] << 8) | (data[start + 1] & 0xff));
                break;

            case 4:
                // INT32
                num = (int) (((data[start] & 0xff) << 24) | ((data[start + 1] & 0xff) << 16)
                        | ((data[start + 2] & 0xff) << 8) | (data[start + 3] & 0xff));
                break;

            case 8:
                // INT64
                num = (((long) (data[start] & 0xff) << 56) | ((long) (data[start + 1] & 0xff) << 48)
                        | ((long) (data[start + 2] & 0xff) << 40) | ((long) (data[start + 3] & 0xff) << 32)
                        | ((long) (data[start + 4] & 0xff) << 24) | ((long) (data[start + 5] & 0xff) << 16)
                        | ((long) (data[start + 6] & 0xff) << 8) | ((long) (data[start + 7] & 0xff)));

                break;

            default:
                throw new IOException("Can't convert integer types. The source type should be one of INT8/16/32/64.");

        }

        // Boundary check
        switch (targetType) {
            case INT8:
                if (num > Byte.MAX_VALUE || num < Byte.MIN_VALUE) {
                    throw new IOException("Source value " + num
                            + " is out of range that INT8 can hold - INT8.MAX_VALUE:" + Byte.MAX_VALUE
                            + ", INT8.MIN_VALUE:" + Byte.MIN_VALUE);
                }
                break;

            case INT16:
                if (num > Short.MAX_VALUE || num < Short.MIN_VALUE) {
                    throw new IOException("Source value " + num
                            + " is out of range that INT16 can hold - INT16.MAX_VALUE:" + Short.MAX_VALUE
                            + ", INT16.MIN_VALUE:" + Short.MIN_VALUE);
                }
                break;

            case INT32:
                if (num > Integer.MAX_VALUE || num < Integer.MIN_VALUE) {
                    throw new IOException("Source value " + num
                            + " is out of range that INT32 can hold - INT32.MAX_VALUE:" + Integer.MAX_VALUE
                            + ", INT32.MIN_VALUE:" + Integer.MIN_VALUE);
                }
                break;

            case INT64:
            default:
                break;
        }

        out.writeByte(targetType.serialize());

        // Write actual target value
        switch (targetTypeLength) {
            case 1:
                // INT8
                out.writeByte((byte) (num & 0xff));
                break;

            case 2:
                // INT16
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            case 4:
                // INT32
                out.writeByte((byte) ((num >> 24) & 0xff));
                out.writeByte((byte) ((num >> 16) & 0xff));
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            case 8:
                // INT64
                out.writeByte((byte) ((num >> 56) & 0xff));
                out.writeByte((byte) ((num >> 48) & 0xff));
                out.writeByte((byte) ((num >> 40) & 0xff));
                out.writeByte((byte) ((num >> 32) & 0xff));
                out.writeByte((byte) ((num >> 24) & 0xff));
                out.writeByte((byte) ((num >> 16) & 0xff));
                out.writeByte((byte) ((num >> 8) & 0xff));
                out.writeByte((byte) (num & 0xff));
                break;

            default:
                throw new IOException("Can't convert integer types. The target type should be one of INT8/16/32/64.");

        }

//        for (int i = targetTypeLength - 1; i >= 0; i--) {
//            out.writeByte((byte) ((num >>> (i * 8)) & 0xFF));
//        }
    }
}
