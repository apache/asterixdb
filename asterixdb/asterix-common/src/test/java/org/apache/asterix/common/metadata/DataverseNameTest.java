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

import static org.apache.asterix.common.functions.FunctionConstants.ASTERIX_NS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.apache.commons.collections4.ListUtils;
import org.apache.hyracks.algebricks.common.utils.Triple;
import org.apache.hyracks.algebricks.core.algebra.functions.AlgebricksBuiltinFunctions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link DataverseName}
 */
public class DataverseNameTest {

    private static final List<String> TEST_BUILTIN_DATAVERSE_NAME_PARAMS = Arrays.asList(
            // 1-part-name
            // default dataverse
            "Default",
            // metadata dataverse
            "Metadata",
            // dataverse for Algebricks functions
            AlgebricksBuiltinFunctions.ALGEBRICKS_NS,
            // dataverse for Asterix functions
            ASTERIX_NS);

    private static final List<String> TEST_BUILTIN_DATAVERSE_INVALID_NAME_PARAMS = Arrays.asList(
            // separator character is not allowed
            "a.b",
            // escape character is not allowed
            "c@d");

    private static final List<Triple<String, String, String>> TEST_SINGLE_PART_NAME_PARAMS = Arrays.asList(
            // <1-part-name, canonical-form, display-form>
            new Triple<>("abz", "abz", "abz"),
            // upper-case letters
            new Triple<>("ABZ", "ABZ", "ABZ"),
            // letters and digits
            new Triple<>("aA09", "aA09", "aA09"),
            // with canonical form escape character
            new Triple<>("a@b", "a@@b", "`a@b`"),
            // with canonical form separator character
            new Triple<>("a.b", "a@.b", "`a.b`"),
            // with canonical form escape and separator characters
            new Triple<>("a@.b", "a@@@.b", "`a@.b`"),
            // with display form escape character
            new Triple<>("a\\b", "a\\b", "`a\\\\b`"));

    private static final List<Triple<List<String>, String, String>> TEST_MULTI_PART_NAME_PARAMS = Arrays.asList(
            // <multi-part-name, canonical-form, display-form>
            new Triple<>(Arrays.asList("aa", "bb", "cc"), "aa.bb.cc", "aa.bb.cc"),
            // mixed case letters, digits
            new Triple<>(Arrays.asList("az", "AZ", "a09Z"), "az.AZ.a09Z", "az.AZ.a09Z"),
            // with canonical form escape character
            new Triple<>(Arrays.asList("a@a@", "@b@b", "@c@c"), "a@@a@@.@@b@@b.@@c@@c", "`a@a@`.`@b@b`.`@c@c`"),
            // with canonical form separator character
            new Triple<>(Arrays.asList("a.a.", ".b.b.", ".c.c"), "a@.a@..@.b@.b@..@.c@.c", "`a.a.`.`.b.b.`.`.c.c`"),
            // with canonical form escape and separator characters
            new Triple<>(Arrays.asList("a@a.", "@b.b@", ".c@c"), "a@@a@..@@b@.b@@.@.c@@c", "`a@a.`.`@b.b@`.`.c@c`"),
            // with canonical form escape and separator characters repeated
            new Triple<>(Arrays.asList("a@@a..", "@@b..b@@", "..c@@c"), "a@@@@a@.@..@@@@b@.@.b@@@@.@.@.c@@@@c",
                    "`a@@a..`.`@@b..b@@`.`..c@@c`"),
            // with display form escape character
            new Triple<>(Arrays.asList("a\\b", "c\\d"), "a\\b.c\\d", "`a\\\\b`.`c\\\\d`"));

    @Test
    public void testBuiltinDataverseName() throws Exception {
        for (String p : TEST_BUILTIN_DATAVERSE_NAME_PARAMS) {
            testBuiltinDataverseNameImpl(p);
        }
    }

    @Test
    public void testSinglePartName() throws Exception {
        for (Triple<String, String, String> t : TEST_SINGLE_PART_NAME_PARAMS) {
            String singlePart = t.first;
            String expectedCanonicalForm = t.second;
            String expectedDisplayForm = t.third;
            testSinglePartNameImpl(singlePart, expectedCanonicalForm, expectedDisplayForm);
        }
    }

    @Test
    public void testMultiPartName() throws Exception {
        // test single part names
        for (Triple<String, String, String> t : TEST_SINGLE_PART_NAME_PARAMS) {
            List<String> parts = Collections.singletonList(t.first);
            String expectedCanonicalForm = t.second;
            String expectedDisplayForm = t.third;
            testMultiPartNameImpl(parts, expectedCanonicalForm, expectedDisplayForm);
        }
        // test multi part names
        for (Triple<List<String>, String, String> t : TEST_MULTI_PART_NAME_PARAMS) {
            List<String> parts = t.first;
            String expectedCanonicalForm = t.second;
            String expectedDisplayForm = t.third;
            testMultiPartNameImpl(parts, expectedCanonicalForm, expectedDisplayForm);
        }
    }

    private void testBuiltinDataverseNameImpl(String singlePart) throws Exception {
        DataverseName dvBuiltin = DataverseName.createBuiltinDataverseName(singlePart);
        DataverseName dv = DataverseName.createSinglePartName(singlePart);
        Assert.assertEquals("same-builtin", dv, dvBuiltin);
        // part = canonical-form = display-form for builtins
        testSinglePartNameImpl(singlePart, singlePart, singlePart);
    }

    private void testSinglePartNameImpl(String singlePart, String expectedCanonicalForm, String expectedDisplayForm)
            throws Exception {
        List<String> parts = Collections.singletonList(singlePart);

        // construction using createSinglePartName()
        DataverseName dvConstr1 = DataverseName.createSinglePartName(singlePart);
        testDataverseNameImpl(dvConstr1, parts, expectedCanonicalForm, expectedDisplayForm);

        // construction using create(list)
        DataverseName dvConstr2 = DataverseName.create(Collections.singletonList(singlePart));
        testDataverseNameImpl(dvConstr2, parts, expectedCanonicalForm, expectedDisplayForm);

        // construction using create(list, from, to)
        DataverseName dvConstr3 = DataverseName.create(Arrays.asList(null, null, singlePart, null, null), 2, 3);
        testDataverseNameImpl(dvConstr3, parts, expectedCanonicalForm, expectedDisplayForm);
    }

    private void testMultiPartNameImpl(List<String> parts, String expectedCanonicalForm, String expectedDisplayForm)
            throws Exception {
        // construction using create(list)
        DataverseName dvConstr1 = DataverseName.create(parts);
        testDataverseNameImpl(dvConstr1, parts, expectedCanonicalForm, expectedDisplayForm);

        // construction using create(list, from, to)
        List<String> dv2InputParts =
                ListUtils.union(ListUtils.union(Arrays.asList(null, null), parts), Arrays.asList(null, null));
        DataverseName dvConstr2 = DataverseName.create(dv2InputParts, 2, 2 + parts.size());
        testDataverseNameImpl(dvConstr2, parts, expectedCanonicalForm, expectedDisplayForm);
    }

    protected void testDataverseNameImpl(DataverseName dataverseName, List<String> parts, String expectedCanonicalForm,
            String expectedDisplayForm) throws Exception {
        boolean isMultiPart = parts.size() > 1;
        Assert.assertEquals("is-multipart", isMultiPart, dataverseName.isMultiPart());

        // test getParts()
        Assert.assertArrayEquals("get-parts-0", parts.toArray(), dataverseName.getParts().toArray());
        List<String> outParts = new ArrayList<>();
        dataverseName.getParts(outParts);
        Assert.assertArrayEquals("get-parts-1", parts.toArray(), outParts.toArray());

        // test canonical form
        String canonicalForm = dataverseName.getCanonicalForm();
        Assert.assertEquals("canonical-form", expectedCanonicalForm, canonicalForm);
        DataverseName dvFromCanonical = DataverseName.createFromCanonicalForm(expectedCanonicalForm);
        Assert.assertEquals("canonical-form-round-trip", dataverseName, dvFromCanonical);
        Assert.assertEquals("canonical-form-round-trip-cmp", 0, dataverseName.compareTo(dvFromCanonical));
        Assert.assertEquals("canonical-form-round-trip-hash", dataverseName.hashCode(), dvFromCanonical.hashCode());

        // test display form
        String displayForm = dataverseName.toString();
        Assert.assertEquals("display-form", expectedDisplayForm, displayForm);
    }

    @Test
    public void testCompare() {
        List<DataverseName> dvList =
                Arrays.asList(DataverseName.createSinglePartName("a"), DataverseName.create(Arrays.asList("a", "a")),
                        DataverseName.createSinglePartName("aa"), DataverseName.createSinglePartName("b"));

        for (int i = 0; i < dvList.size() - 1; i++) {
            for (int j = i + 1; j < dvList.size(); j++) {
                testCompareImpl(dvList.get(i), dvList.get(j));
            }
        }
    }

    private void testCompareImpl(DataverseName left, DataverseName right) {
        String label = left.getCanonicalForm() + " ? " + right.getCanonicalForm();
        Assert.assertNotEquals(left, right);
        Assert.assertTrue(label, left.compareTo(right) < 0);
        Assert.assertTrue(label, right.compareTo(left) > 0);
    }

    @Test
    public void testExceptions() {
        // 1. Invalid names for builtin dataverses
        for (String p : TEST_BUILTIN_DATAVERSE_INVALID_NAME_PARAMS) {
            testInvalidBuiltinDataverseNameImpl(p);
        }
        // 2. NullPointerException
        testRuntimeException(() -> DataverseName.create(null), NullPointerException.class);
        testRuntimeException(() -> DataverseName.create(null, 0, 0), NullPointerException.class);
        testRuntimeException(() -> DataverseName.create(null, 0, 1), NullPointerException.class);
        testRuntimeException(() -> DataverseName.create(null, 0, 2), NullPointerException.class);
        testRuntimeException(() -> DataverseName.createSinglePartName(null), NullPointerException.class);
        testRuntimeException(() -> DataverseName.createBuiltinDataverseName(null), NullPointerException.class);
        testRuntimeException(() -> DataverseName.createFromCanonicalForm(null), NullPointerException.class);
        testRuntimeException(() -> DataverseName.create(Collections.singletonList(null)), NullPointerException.class);
        testRuntimeException(() -> DataverseName.create(Arrays.asList(null, null)), NullPointerException.class);
        // 3. IndexOutOfBoundsException
        testRuntimeException(() -> DataverseName.create(Collections.emptyList(), 0, 1),
                IndexOutOfBoundsException.class);
        testRuntimeException(() -> DataverseName.create(Collections.emptyList(), 0, 2),
                IndexOutOfBoundsException.class);
        // 4. IllegalArgumentException
        testRuntimeException(() -> DataverseName.create(Collections.emptyList()), IllegalArgumentException.class);
        testRuntimeException(() -> DataverseName.create(Collections.emptyList(), 0, 0), IllegalArgumentException.class);
        testRuntimeException(() -> DataverseName.create(Arrays.asList("a", "b", "c"), 2, 1),
                IllegalArgumentException.class);
    }

    private <E extends RuntimeException> void testRuntimeException(Supplier<DataverseName> supplier,
            Class<E> exceptionClass) {
        try {
            supplier.get();
            Assert.fail("Did not get expected exception " + exceptionClass.getName());
        } catch (RuntimeException e) {
            if (!exceptionClass.isInstance(e)) {
                try {
                    Assert.fail(
                            "Expected to catch " + exceptionClass.getName() + ", but caught " + e.getClass().getName());
                } catch (AssertionError ae) {
                    ae.initCause(e);
                    throw ae;
                }
            }
        }
    }

    private void testInvalidBuiltinDataverseNameImpl(String singlePart) {
        try {
            DataverseName.createBuiltinDataverseName(singlePart);
            Assert.fail(singlePart);
        } catch (IllegalArgumentException e) {
            // this error is expected
        }
    }
}
