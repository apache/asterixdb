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

package org.apache.hyracks.tests.am.btree;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.data.std.accessors.UTF8StringBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.UTF8StringPointable;
import org.apache.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import org.apache.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;

public class DataSetConstants {

    public static final RecordDescriptor inputRecordDesc =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    public static final IValueParserFactory[] inputParserFactories = new IValueParserFactory[] {
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
            UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE };

    // field, type and key declarations for primary index
    public static int[] primaryFieldPermutation = { 0, 1, 2, 4, 5, 7 };
    public static final int[] primaryFilterFields = new int[] { 0 };
    public static final int[] primaryBtreeFields = new int[] { 0, 1, 2, 3, 4, 5 };

    public static final ITypeTraits[] filterTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
    public static final IBinaryComparatorFactory[] filterCmpFactories =
            new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };

    public static final ITypeTraits[] primaryTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS,
            UTF8StringPointable.TYPE_TRAITS, UTF8StringPointable.TYPE_TRAITS, UTF8StringPointable.TYPE_TRAITS,
            UTF8StringPointable.TYPE_TRAITS, UTF8StringPointable.TYPE_TRAITS };

    public static final IBinaryComparatorFactory[] primaryComparatorFactories =
            new IBinaryComparatorFactory[] { UTF8StringBinaryComparatorFactory.INSTANCE };
    public static final int primaryKeyFieldCount = primaryComparatorFactories.length;

    public static final int[] primaryBloomFilterKeyFields = new int[] { 0 };

    public static final RecordDescriptor primaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    public static final RecordDescriptor primaryAndFilterRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });

    // field, type and key declarations for secondary indexes

    public static final int secondaryKeyFieldCount = 2;
    public static final int[] secondaryFieldPermutationA = { 3, 0 };
    public static final int[] secondaryFieldPermutationB = { 4, 0 };
    public static final int[] secondaryFilterFields = new int[] { 1 };
    public static final int[] secondaryBtreeFields = new int[] { 0, 1 };
    public static final int[] secondaryBloomFilterKeyFields = new int[] { 0, 1 };

    public static final ITypeTraits[] secondaryTypeTraits =
            new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS, UTF8StringPointable.TYPE_TRAITS };

    public static final IBinaryComparatorFactory[] secondaryComparatorFactories = new IBinaryComparatorFactory[] {
            UTF8StringBinaryComparatorFactory.INSTANCE, UTF8StringBinaryComparatorFactory.INSTANCE };

    public static final RecordDescriptor secondaryRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer() });
    public static final RecordDescriptor secondaryWithFilterRecDesc =
            new RecordDescriptor(new ISerializerDeserializer[] { new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer(), new UTF8StringSerializerDeserializer(),
                    new UTF8StringSerializerDeserializer() });
}
