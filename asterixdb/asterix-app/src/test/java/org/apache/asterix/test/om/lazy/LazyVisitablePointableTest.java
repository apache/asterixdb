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
package org.apache.asterix.test.om.lazy;

import static org.apache.hyracks.util.file.FileUtil.joinPath;

import java.io.FileInputStream;
import java.io.IOException;

import org.apache.asterix.external.parser.JSONDataParser;
import org.apache.asterix.om.lazy.AbstractLazyVisitablePointable;
import org.apache.asterix.om.lazy.RecordLazyVisitablePointable;
import org.apache.asterix.om.lazy.TypedRecordLazyVisitablePointable;
import org.apache.asterix.om.pointables.ARecordVisitablePointable;
import org.apache.asterix.om.pointables.base.DefaultOpenFieldType;
import org.apache.asterix.om.pointables.base.IVisitablePointable;
import org.apache.asterix.om.pointables.cast.ACastVisitor;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.runtime.evaluators.comparisons.DeepEqualAssessor;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.util.ArrayBackedValueStorage;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;

/**
 * Test the operations of {@link AbstractLazyVisitablePointable}
 */
public class LazyVisitablePointableTest {
    private static final String BASE_DIR;
    private static final String[] FILE_PATHS;
    private final JSONDataParser parser;
    private final ACastVisitor castVisitor;
    private final RecordTypeInference schemaInference;
    private final DeepEqualAssessor deepEqualAssessor;
    private final RecordLazyVisitablePointable openLazyPointable;
    private final ARecordVisitablePointable openPointable;
    private final ArrayBackedValueStorage recordStorage;
    private final Triple<IVisitablePointable, IAType, Boolean> arg;

    static {
        BASE_DIR = "data";
        FILE_PATHS = new String[] { joinPath(BASE_DIR, "hdfs", "parquet", "dummy_tweet.json"),
                joinPath(BASE_DIR, "nested01", "person2.adm"), joinPath(BASE_DIR, "yelp-checkin", "use-case-1.json"),
                joinPath(BASE_DIR, "yelp-checkin", "use-case-2.json"),
                joinPath(BASE_DIR, "yelp-checkin", "use-case-3.json"),
                joinPath(BASE_DIR, "yelp-checkin", "use-case-4.json") };
    }

    public LazyVisitablePointableTest() {
        parser = new JSONDataParser(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE, new JsonFactory());
        castVisitor = new ACastVisitor();
        schemaInference = new RecordTypeInference();
        deepEqualAssessor = new DeepEqualAssessor();
        openLazyPointable = new RecordLazyVisitablePointable(true);
        openPointable = new ARecordVisitablePointable(DefaultOpenFieldType.NESTED_OPEN_RECORD_TYPE);
        recordStorage = new ArrayBackedValueStorage();
        arg = new Triple<>(null, null, null);
        arg.third = Boolean.FALSE;
    }

    private void prepareParser(String path) throws IOException {
        FileInputStream inputStream = new FileInputStream(path);
        parser.setInputStream(inputStream);
    }

    private void inferCastAndCompare() throws HyracksDataException {
        recordStorage.reset();
        while (parser.parse(recordStorage.getDataOutput())) {
            openLazyPointable.set(recordStorage);

            //Infer the schema
            ARecordType inferredFromOpen = (ARecordType) openLazyPointable.accept(schemaInference, "fromOpen");
            ARecordVisitablePointable closedPointable = new ARecordVisitablePointable(inferredFromOpen);
            arg.first = closedPointable;
            arg.second = inferredFromOpen;

            //Cast to closed using the inferred type
            openPointable.set(recordStorage);
            openPointable.accept(castVisitor, arg);
            //Ensure both closed and open records are the same
            Assert.assertTrue(deepEqualAssessor.isEqual(openPointable, closedPointable));

            //Ensure lazy pointable can handle closed types
            TypedRecordLazyVisitablePointable closedLazyPointable =
                    new TypedRecordLazyVisitablePointable(inferredFromOpen);
            closedLazyPointable.set(closedPointable);
            //Infer the type (again) but from a closed type
            ARecordType inferredFromClosed = (ARecordType) closedLazyPointable.accept(schemaInference, "fromClosed");
            //Ensure both inferred types are the same
            Assert.assertTrue(inferredFromOpen.deepEqual(inferredFromClosed));
            recordStorage.reset();
        }
    }

    @Test
    public void runTest() throws IOException {
        for (String path : FILE_PATHS) {
            prepareParser(path);
            inferCastAndCompare();
        }
    }
}
