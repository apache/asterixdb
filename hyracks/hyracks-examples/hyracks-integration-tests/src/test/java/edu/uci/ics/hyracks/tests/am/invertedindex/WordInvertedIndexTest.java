/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.tests.am.invertedindex;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ConstantMergePolicyFactory;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.NoOpIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.SynchronousSchedulerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.common.impls.ThreadCountingOperationTrackerProvider;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.dataflow.LSMInvertedIndexDataflowHelperFactory;

public class WordInvertedIndexTest extends AbstractfWordInvertedIndexTest {

    @Override
    protected void prepare() {
        // Field declarations and comparators for tokens.
        tokenTypeTraits = new ITypeTraits[] { UTF8StringPointable.TYPE_TRAITS };
        tokenComparatorFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };

        tokenizerRecDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        sortComparatorFactories = new IBinaryComparatorFactory[] {
                PointableBinaryComparatorFactory.of(UTF8StringPointable.FACTORY),
                PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) };

        invertedIndexDataflowHelperFactory = new LSMInvertedIndexDataflowHelperFactory(virtualBufferCacheProvider,
                new ConstantMergePolicyFactory(), MERGE_POLICY_PROPERTIES,
                ThreadCountingOperationTrackerProvider.INSTANCE, SynchronousSchedulerProvider.INSTANCE,
                NoOpIOOperationCallback.INSTANCE, DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE);
    }

    @Override
    protected boolean addNumTokensKey() {
        return false;
    }
}
