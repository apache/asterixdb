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

package org.apache.asterix.dataflow.data.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.asterix.om.types.AOrderedListType;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AUnorderedListType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for TypeResolverUtil.
 */
public class TypeResolverUtilTest {

    @Test
    public void testRecordType() {
        // Constructs input types.
        ARecordType leftRecordType = new ARecordType(null, new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, false, null);
        ARecordType rightRecordType = new ARecordType(null, new String[] { "b", "c" },
                new IAType[] { BuiltinType.AINT32, BuiltinType.ABINARY }, false, null);

        // Resolves input types to a generalized type.
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftRecordType);
        inputTypes.add(rightRecordType);
        ARecordType resolvedType = (ARecordType) TypeResolverUtil.resolve(inputTypes);

        // Constructs the expected type.
        Set<String> possibleAdditionalFields = new HashSet<>();
        possibleAdditionalFields.add("a");
        possibleAdditionalFields.add("c");
        ARecordType expectedType = new ARecordType(null, new String[] { "b" }, new IAType[] { BuiltinType.AINT32 },
                true, possibleAdditionalFields);

        // Compares the resolved type with the expected type.
        Assert.assertEquals(resolvedType, expectedType);
        Assert.assertEquals(resolvedType.getAllPossibleAdditonalFieldNames(),
                expectedType.getAllPossibleAdditonalFieldNames());
    }

    @Test
    public void testIsmophicRecordType() {
        // Constructs input types.
        ARecordType leftRecordType = new ARecordType(null, new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, false, null);
        ARecordType rightRecordType = new ARecordType(null, new String[] { "b", "a" },
                new IAType[] { BuiltinType.AINT32, BuiltinType.ASTRING }, false, null);

        // Resolves input types to a generalized type.
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftRecordType);
        inputTypes.add(rightRecordType);
        ARecordType resolvedType = (ARecordType) TypeResolverUtil.resolve(inputTypes);

        // Compares the resolved type with the expected type.
        Assert.assertEquals(resolvedType, leftRecordType);
    }

    @Test
    public void testNestedRecordType() {
        // Constructs input types.
        ARecordType leftRecordType =
                new ARecordType("null", new String[] { "a", "b" },
                        new IAType[] { BuiltinType.ASTRING,
                                new ARecordType(null, new String[] { "c", "d" },
                                        new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, false, null) },
                        false, null);
        ARecordType rightRecordType =
                new ARecordType("null", new String[] { "a", "b" },
                        new IAType[] { BuiltinType.ASTRING,
                                new ARecordType(null, new String[] { "d", "e" },
                                        new IAType[] { BuiltinType.AINT32, BuiltinType.AINT32 }, false, null) },
                        false, null);

        // Resolves input types to a generalized type.
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftRecordType);
        inputTypes.add(rightRecordType);
        ARecordType resolvedType = (ARecordType) TypeResolverUtil.resolve(inputTypes);
        ARecordType nestedRecordType = (ARecordType) resolvedType.getFieldType("b");

        // Constructs the expected type.
        Set<String> nestedPossibleAdditionalFields = new HashSet<>();
        nestedPossibleAdditionalFields.add("c");
        nestedPossibleAdditionalFields.add("e");
        ARecordType expectedType =
                new ARecordType(null, new String[] { "a", "b" },
                        new IAType[] { BuiltinType.ASTRING, new ARecordType(null, new String[] { "d" },
                                new IAType[] { BuiltinType.AINT32 }, true, nestedPossibleAdditionalFields) },
                        false, null);

        // Compares the resolved type with the expected type.
        Assert.assertEquals(expectedType, resolvedType);
        Assert.assertEquals(nestedRecordType.getAllPossibleAdditonalFieldNames(), nestedPossibleAdditionalFields);
    }

    @Test
    public void testOrderedListType() {
        // Constructs input types.
        ARecordType leftRecordType = new ARecordType("null", new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, true, Collections.singleton("d"));
        AOrderedListType leftListType = new AOrderedListType(leftRecordType, "null");
        ARecordType rightRecordType = new ARecordType("null", new String[] { "b", "c" },
                new IAType[] { BuiltinType.AINT32, BuiltinType.ABINARY }, true, Collections.singleton("e"));
        AOrderedListType rightListType = new AOrderedListType(rightRecordType, "null");

        // Gets the actual resolved type.
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftListType);
        inputTypes.add(rightListType);
        AbstractCollectionType resolvedType = (AbstractCollectionType) TypeResolverUtil.resolve(inputTypes);
        ARecordType resolvedRecordType = (ARecordType) resolvedType.getItemType();

        // Gets the expected generalized type.
        Set<String> possibleAdditionalFields = new HashSet<>();
        possibleAdditionalFields.add("a");
        possibleAdditionalFields.add("c");
        possibleAdditionalFields.add("d");
        possibleAdditionalFields.add("e");
        ARecordType expectedRecordType = new ARecordType(null, new String[] { "b" },
                new IAType[] { BuiltinType.AINT32 }, true, possibleAdditionalFields);
        AOrderedListType expectedListType = new AOrderedListType(expectedRecordType, null);

        // Compares the resolved type and the expected type.
        Assert.assertEquals(resolvedType, expectedListType);
        Assert.assertEquals(resolvedRecordType.getAllPossibleAdditonalFieldNames(),
                expectedRecordType.getAllPossibleAdditonalFieldNames());
    }

    @Test
    public void testUnorderedListType() {
        // Constructs input types.
        ARecordType leftRecordType = new ARecordType(null, new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, true, Collections.singleton("d"));
        AUnorderedListType leftListType = new AUnorderedListType(leftRecordType, null);
        AUnorderedListType rightListType = new AUnorderedListType(BuiltinType.ASTRING, null);

        // Gets the actual resolved type.
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftListType);
        inputTypes.add(rightListType);
        AbstractCollectionType resolvedType = (AbstractCollectionType) TypeResolverUtil.resolve(inputTypes);

        // Compares the resolved type and the expected type.
        Assert.assertEquals(resolvedType, new AUnorderedListType(BuiltinType.ANY, null));
    }

    @Test
    public void testNullType() {
        ARecordType leftRecordType = new ARecordType(null, new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, false, null);
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftRecordType);
        inputTypes.add(BuiltinType.ANULL);
        IAType resolvedType = TypeResolverUtil.resolve(inputTypes);
        Assert.assertEquals(resolvedType, AUnionType.createUnknownableType(leftRecordType));
    }

    @Test
    public void testMissingType() {
        ARecordType leftRecordType = new ARecordType(null, new String[] { "a", "b" },
                new IAType[] { BuiltinType.ASTRING, BuiltinType.AINT32 }, false, null);
        List<IAType> inputTypes = new ArrayList<>();
        inputTypes.add(leftRecordType);
        inputTypes.add(BuiltinType.AMISSING);
        IAType resolvedType = TypeResolverUtil.resolve(inputTypes);
        Assert.assertEquals(resolvedType, AUnionType.createUnknownableType(leftRecordType));
    }

}
