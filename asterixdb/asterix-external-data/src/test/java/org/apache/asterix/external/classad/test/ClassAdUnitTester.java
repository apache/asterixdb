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
package org.apache.asterix.external.classad.test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.asterix.external.classad.AMutableCharArrayString;
import org.apache.asterix.external.classad.ClassAd;
import org.apache.asterix.external.classad.ClassAdTime;
import org.apache.asterix.external.classad.ExprList;
import org.apache.asterix.external.classad.ExprTree;
import org.apache.asterix.external.classad.Literal;
import org.apache.asterix.external.classad.Util;
import org.apache.asterix.external.classad.Value;
import org.apache.asterix.external.classad.Value.ValueType;
import org.apache.asterix.external.classad.object.pool.ClassAdObjectPool;
import org.apache.asterix.external.library.ClassAdParser;
import org.apache.asterix.om.base.AMutableDouble;
import org.apache.asterix.om.base.AMutableInt32;
import org.apache.asterix.om.base.AMutableInt64;
import org.apache.asterix.om.base.AMutableString;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class ClassAdUnitTester {
    static class Parameters {
        public boolean debug;
        public boolean verbose;
        public boolean veryVerbose;

        public boolean checkAll;
        public boolean checkParsing;
        public boolean checkClassad;
        public boolean checkExprlist;
        public boolean checkValue;
        public boolean checkLiteral;
        public boolean checkMatch;
        public boolean checkOperator;
        public boolean checkCollection;
        public boolean checkUtils;

        /*********************************************************************
         * Function: Parameters::ParseCommandLine
         * Purpose: This parses the command line. Note that it will exit
         * if there are any problems.
         *********************************************************************/
        public void ParseCommandLine(int argc, String[] argv) {

            boolean selectedTest = false;
            boolean help = false;

            // First we set up the defaults.
            debug = false;
            verbose = false;
            veryVerbose = false;
            checkAll = false;
            checkParsing = false;
            checkClassad = false;
            checkExprlist = false;
            checkValue = false;
            checkLiteral = false;
            checkMatch = false;
            checkOperator = false;
            checkCollection = false;
            checkUtils = false;

            // Then we parse to see what the user wants.
            for (int argIndex = 1; argIndex < argc; argIndex++) {
                if (argv[argIndex].equalsIgnoreCase("-h") || argv[argIndex].equalsIgnoreCase("-help")) {
                    help = true;
                    break;
                } else if (argv[argIndex].equalsIgnoreCase("-d") || argv[argIndex].equalsIgnoreCase("-debug")) {
                    debug = true;
                } else if (argv[argIndex].equalsIgnoreCase("-v") || argv[argIndex].equalsIgnoreCase("-verbose")) {
                    verbose = true;
                } else if (argv[argIndex].equalsIgnoreCase("-vv") || argv[argIndex].equalsIgnoreCase("-veryverbose")) {
                    verbose = true;
                    veryVerbose = true;
                } else if (argv[argIndex].equalsIgnoreCase("-all")) {
                    checkAll = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-parsing")) {
                    checkParsing = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-classad")) {
                    checkClassad = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-epxrlist")) {
                    checkExprlist = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-value")) {
                    checkValue = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-literal")) {
                    checkLiteral = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-match")) {
                    checkMatch = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-operator")) {
                    checkOperator = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-collection")) {
                    checkCollection = true;
                    selectedTest = true;
                } else if (argv[argIndex].equalsIgnoreCase("-utils")) {
                    checkUtils = true;
                    selectedTest = true;
                } else {
                    System.out.println("Unknown argument: " + argv[argIndex]);
                    help = true;
                }
            }

            if (help) {
                System.out.println("Usage: classad_unit_tester [options]");
                System.out.println();
                System.out.println("Basic options:");
                System.out.println("    -h  | -help:        print help");
                System.out.println("    -d  | -debug:       debug");
                System.out.println("    -v  | -verbose:     verbose output");
                System.out.println("    -vv | -veryverbose: very verbose output");
                System.out.println();
                System.out.println("Test selectors:");
                System.out.println("    -all:        all tests listed below (the default)");
                System.out.println("    -parsing:    test non-ClassAd parsing.");
                System.out.println("    -classad:    test the ClassAd class.");
                System.out.println("    -exprlist:   test the ExprList class.");
                System.out.println("    -value:      test the Value class.");
                System.out.println("    -literal:    test the Literal class.");
                System.out.println("    -match:      test the MatchClassAd class.");
                System.out.println("    -operator:   test the Operator class.");
                System.out.println("    -collection: test the Collection class.");
                System.out.println("    -utils:      test little utilities.");
                System.exit(1);
            }
            if (!selectedTest) {
                checkAll = true;
            }

            return;
        }
    }

    static class Results {
        public int numberOfErrors;
        public int numberOfTests;
        public boolean verbose;
        public boolean very_verbose;

        public Results(Parameters parameters) {
            numberOfErrors = 0;
            numberOfTests = 0;
            verbose = parameters.verbose;
            very_verbose = parameters.veryVerbose;
        }

        public void AddSuccessfulTest(String name, String testLine) {
            numberOfTests++;
            if (very_verbose) {
                System.out.println("SUCCESS: " + name + " on test line " + testLine);
            }
        }

        public void AddFailedTest(String name, String testLine) {
            numberOfErrors++;
            numberOfTests++;

            System.out.println("FAILURE " + name + " on test line " + testLine);
            assert (false);
        }

        public void GetResults(AMutableInt32 numberOfErrors, AMutableInt32 numberOfTests) {
            numberOfErrors.setValue(this.numberOfErrors);
            numberOfTests.setValue(this.numberOfTests);
        }
    }

    /*********************************************************************
     * Function: main
     * Purpose: The main control loop.
     *
     * @throws IOException
     *********************************************************************/
    public static boolean test(int argc, String[] argv, ClassAdObjectPool objectPool) throws IOException {
        AMutableInt32 numberOfErrors = new AMutableInt32(0);
        AMutableInt32 numberOfTests = new AMutableInt32(0);
        boolean have_errors;
        Parameters parameters = new Parameters();

        /* ----- Setup ----- */
        printVersion();
        parameters.ParseCommandLine(argc, argv);
        Results results = new Results(parameters);

        /* ----- Run tests ----- */
        if (parameters.checkAll || parameters.checkParsing) {
            testParsing(parameters, results, objectPool);
        }

        if (parameters.checkAll || parameters.checkClassad) {
            try {
                testClassad(parameters, results, objectPool);
            } catch (Throwable th) {
                th.printStackTrace();
                throw th;
            }
        }
        if (parameters.checkAll || parameters.checkExprlist) {
            testExprList(parameters, results, objectPool);
        }
        if (parameters.checkAll || parameters.checkValue) {
            testValue(parameters, results, objectPool);
        }
        if (parameters.checkAll || parameters.checkLiteral) {
        }
        if (parameters.checkAll || parameters.checkMatch) {
        }
        if (parameters.checkAll || parameters.checkOperator) {
        }
        if (parameters.checkAll || parameters.checkCollection) {
            // test_collection(parameters, results);
        }
        if (parameters.checkAll || parameters.checkUtils) {
            testUtils(parameters, results);
        }

        /* ----- Report ----- */
        System.out.println();
        results.GetResults(numberOfErrors, numberOfTests);
        if (numberOfErrors.getIntegerValue() > 0) {
            have_errors = true;
            System.out.println("Finished with errors: ");
            System.out.println("    " + numberOfErrors + " errors");
            System.out.println("    " + numberOfTests + " tests");
        } else {
            have_errors = false;
            System.out.println("Finished with no errors.");
            System.out.println("    " + numberOfTests + " tests");
        }
        return have_errors;
    }

    public static void test(String name, boolean test, String testLine, Results results) {
        if (test) {
            results.AddSuccessfulTest(name, testLine);
        } else {
            results.AddFailedTest(name, testLine);
        }
    }

    public static void test(String name, boolean test, Results results) {
        test(name, test, name, results);
    }

    /*********************************************************************
     * Function: test_parsing
     * Purpose: Test parsing that isn't ClassAd-specific. (ClassAd-specific
     * is in test_clasad
     *
     * @throws IOException
     *********************************************************************/
    public static void testParsing(Parameters parameters, Results results, ClassAdObjectPool objectPool)
            throws IOException {
        ClassAdParser parser = new ClassAdParser(objectPool);
        ExprTree tree;

        // My goal is to ensure that these expressions don't crash
        // They should also return a null tree
        tree = parser.ParseExpression("true || false || ;");
        test("Bad or doesn't crash & isn't bogus", tree == null, "true || false || ;", results);

        tree = parser.ParseExpression("true && false && ;");
        test("Bad and doesn't crash & isn't bogus", tree == null, "true && false && ;", results);

        tree = parser.ParseExpression("3 | 4 | ;");
        test("Bad and doesn't crash & isn't bogus", tree == null, "3 | 4 | ;", results);

        tree = parser.ParseExpression("3 ^ 4 ^ ;");
        test("Bad exclusive or doesn't crash & isn't bogus", tree == null, "3 ^ 4 ^ ;", results);

        tree = parser.ParseExpression("3 & 4 & ;");
        test("Bad bitwise and doesn't crash & isn't bogus", tree == null, "3 & 4 & ;", results);

        tree = parser.ParseExpression("3 == 4 ==  ;");
        test("Bad equality doesn't crash & isn't bogus", tree == null, "3 == 4 ==  ;", results);

        tree = parser.ParseExpression("1 < 3 < ;");
        test("Bad relational doesn't crash & isn't bogus", tree == null, "1 < 3 < ;", results);

        tree = parser.ParseExpression("1 + 3 + ;");
        test("Bad shift doesn't crash & isn't bogus", tree == null, "1 + 3 + ;", results);

        tree = parser.ParseExpression("1 + 3 + ;");
        test("Bad additive doesn't crash & isn't bogus", tree == null, "1 + 3 + ;", results);

        tree = parser.ParseExpression("1 * 3 * ;");
        test("Bad multiplicative doesn't crash & isn't bogus", tree == null, "1 * 3 * ;", results);
    }

    /*********************************************************************
     * Function: test_classad
     * Purpose: Test the ClassAd class.
     * @param objectPool
     *
     * @throws IOException
     *********************************************************************/
    public static void testClassad(Parameters parameters, Results results, ClassAdObjectPool objectPool)
            throws IOException {
        ClassAdParser parser = new ClassAdParser(objectPool);
        boolean haveAttribute;
        boolean success;

        System.out.println("Testing the ClassAd class...");

        String input_basic =
                "[ A = 3; B = 4.0; C = \"babyzilla\"; D = true; E = {1}; F = [ AA = 3; ]; G =\"deleteme\";]";
        ClassAd basic = new ClassAd(objectPool);
        AMutableInt64 i = new AMutableInt64(0);
        MutableBoolean b = new MutableBoolean();
        AMutableDouble r = new AMutableDouble(0);
        AMutableCharArrayString s = new AMutableCharArrayString();
        ClassAd c = new ClassAd(objectPool);
        // ExprList *l;

        basic = parser.parseClassAd(input_basic);

        /* ----- Test EvaluateAttr* ----- */
        haveAttribute = basic.evaluateAttrInt("A", i);
        test("Have attribute A", (haveAttribute == true), "test_classad 1", results);
        test("A is 3", (i.getLongValue() == 3), "test_classad 2", results);

        haveAttribute = basic.evaluateAttrReal("B", r);
        test("Have attribute B", (haveAttribute == true), "test_classad 3", results);
        test("B is 4.0", (r.getDoubleValue() == 4.0), "test_classad 4", results);

        haveAttribute = basic.evaluateAttrString("C", s);
        test("Have attribute C", (haveAttribute == true), "test_classad 5", results);
        test("C is 'babyzilla'", (s.compareTo("babyzilla") == 0), "test_classad 6", results);

        haveAttribute = basic.evaluateAttrBool("D", b);
        test("Have attribute D", (haveAttribute == true), "test_classad 7", results);
        test("D is true", (b.booleanValue() == true), "test_classad 8", results);

        /* ----- Test basic insert and delete ----- */
        success = basic.insertAttr("new", 4);
        test("InsertAttr claims to have worked", (success == true), "test_classad 9", results);
        haveAttribute = basic.evaluateAttrInt("new", i);
        test("Have new attribute", (haveAttribute == true), "test_classad 10", results);
        test("new attribute is 4", i.getLongValue() == 4, "test_classad 11", results);

        success = basic.delete("new");
        test("Delete claims to have worked", (success == true), "test_classad 12", results);
        haveAttribute = basic.evaluateAttrInt("new", i);
        test("New attribute was deleted", (haveAttribute == false), "test_classad 13", results);

        success = basic.delete("G");
        test("DELETE claims to have worked", (success == true), "test_classad 14", results);
        haveAttribute = basic.evaluateAttrString("G", s);
        test("Attribute G was deleted", (haveAttribute == false), "test_classad 15", results);

        basic = null;

        /* ----- Test GetExternalReferences ----- */
        String inputRef =
                "[ Rank=Member(\"LCG-2_1_0\",other.Environment) ? other.Time/seconds : other.Time/minutes; minutes=60; ]";
        TreeSet<String> refs = new TreeSet<String>();
        ExprTree rank;

        c = parser.parseClassAd(inputRef);
        test("Made classad_ref", (c != null), "Test GetExternalReferences 1", results);
        if (c != null) {
            rank = c.lookup("Rank");
            test("Rank exists", (rank != null), "Test GetExternalReferences 2", results);

            if (rank != null) {
                boolean haveReferences;
                if ((haveReferences = c.getExternalReferences(rank, refs, true))) {
                    test("have_references", (haveReferences == true), "Test GetExternalReferences 3", results);

                    if (haveReferences) {
                        boolean haveEnvironment;
                        boolean haveTime;
                        boolean haveSeconds;
                        boolean haveOther;
                        haveEnvironment = false;
                        haveTime = false;
                        haveSeconds = false;
                        haveOther = false;
                        for (String entry : refs) {
                            if (entry.compareTo("other.Environment") == 0) {
                                haveEnvironment = true;
                            } else if (entry.compareTo("other.Time") == 0) {
                                haveTime = true;
                            } else if (entry.compareTo("seconds") == 0) {
                                haveSeconds = true;
                            } else {
                                haveOther = true;
                            }
                        }
                        test("Have external reference to Environment", (haveEnvironment == true),
                                "Test GetExternalReferences 4", results);
                        test("Have external reference to Time", (haveTime == true), "Test GetExternalReferences 5",
                                results);
                        test("Have external reference to seconds", (haveSeconds == true),
                                "Test GetExternalReferences 6", results);
                        test("Have no other external references", (haveOther != true), "Test GetExternalReferences 7",
                                results);
                    }
                }
            }
            c = null;
        }

        // This ClassAd may cause problems. Perhaps a memory leak.
        // This test is only useful when run under valgrind.
        String memoryProblemClassad =
                "[ Updates = [status = \"request_completed\"; timestamp = absTime(\"2004-12-16T18:10:59-0600]\")] ]";
        c = parser.parseClassAd(memoryProblemClassad);

        /* ----- Test Parsing multiple ClassAds ----- */
        String twoClassads = "[ a = 3; ][ b = 4; ]";
        ClassAd classad1 = new ClassAd(objectPool);
        ClassAd classad2 = new ClassAd(objectPool);
        AMutableInt32 offset = new AMutableInt32(0);

        parser.parseClassAd(twoClassads, classad1, offset);
        test("Have good offset #1", offset.getIntegerValue() == 10, "Test Parsing multiple ClassAds 1", results);
        parser.parseClassAd(twoClassads, classad2, offset);
        test("Have good offset #2", offset.getIntegerValue() == 20, "Test Parsing multiple ClassAds 2", results);

        /* ----- Test chained ClassAds ----- */
        // classad1 and classad2 from above test are used.
        ClassAd classad3 = new ClassAd(objectPool);

        classad1.chainToAd(classad2);
        test("classad1's parent is classad2", classad1.getChainedParentAd().equals(classad2), "Test chained ClassAds 1",
                results);
        haveAttribute = classad1.evaluateAttrInt("b", i);
        test("chain has attribute b from parent", (haveAttribute == true), "Test chained ClassAds 2", results);
        test("chain attribute b from parent is 4", (i.getLongValue() == 4), "Test chained ClassAds 3", results);

        haveAttribute = classad1.evaluateAttrInt("a", i);
        test("chain has attribute a from self", (haveAttribute == true), "Test chained ClassAds 4", results);
        test("chain attribute a is 3", (i.getLongValue() == 3), "Test chained ClassAds 5", results);

        // Now we modify classad2 (parent) to contain "a".
        success = classad2.insertAttr("a", 7);
        test("insert a into parent", (success == true), "Test chained ClassAds 6", results);
        haveAttribute = classad1.evaluateAttrInt("a", i);
        test("chain has attribute a from self (overriding parent)", (haveAttribute == true), "Test chained ClassAds 7",
                results);
        test("chain attribute a is 3 (overriding parent)", (i.getLongValue() == 3), "Test chained ClassAds 8", results);
        haveAttribute = classad2.evaluateAttrInt("a", i);
        test("chain parent has attribute a", (haveAttribute == true), "Test chained ClassAds 9", results);
        test("chain parent attribute a is 7", (i.getLongValue() == 7), "Test chained ClassAds 10", results);

        success = classad3.copyFromChain(classad1);
        test("copy from chain succeeded", (success == true), "Test chained ClassAds 11", results);
        haveAttribute = classad3.evaluateAttrInt("b", i);
        test("copy of chain has attribute b", (haveAttribute == true), "Test chained ClassAds 12", results);
        test("copy of chain has attribute b==4", (i.getLongValue() == 4), "Test chained ClassAds 13", results);

        success = classad3.insertAttr("c", 6);
        test("insert into copy of chain succeeded", (success == true), "Test chained ClassAds 14", results);
        classad3.copyFromChain(classad1);
        haveAttribute = classad3.evaluateAttrInt("c", i);
        test("copy of chain is clean", (haveAttribute == false), "Test chained ClassAds 15", results);
        classad3.insertAttr("c", 6);
        success = classad3.updateFromChain(classad1);
        test("update from chain succeeded", (success == true), "Test chained ClassAds 16", results);
        haveAttribute = classad3.evaluateAttrInt("c", i);
        test("update from chain is merged", (haveAttribute == true), "Test chained ClassAds 17", results);
        test("update from chain has attribute c==6", (i.getLongValue() == 6), "Test chained ClassAds 18", results);
    }

    /*********************************************************************
     * Function: test_exprlist
     * Purpose: Test the ExprList class.
     *
     * @throws IOException
     *********************************************************************/
    public static void testExprList(Parameters parameters, Results results, ClassAdObjectPool objectPool)
            throws IOException {
        System.out.println("Testing the ExprList class...");

        Literal literal10;
        Literal literal20;
        Literal literal21;

        List<ExprTree> vector1 = new ArrayList<ExprTree>();
        List<ExprTree> vector2 = new ArrayList<ExprTree>();

        ExprList list0;
        ExprList list0Copy;
        ExprList list1;
        ExprList list1Copy;
        ExprList list2;
        ExprList list2Copy;

        /* ----- Setup Literals, the vectors, then ExprLists ----- */
        literal10 = Literal.createReal("1.0", objectPool);
        literal20 = Literal.createReal("2.0", objectPool);
        literal21 = Literal.createReal("2.1", objectPool);

        vector1.add(literal10);
        vector2.add(literal20);
        vector2.add(literal21);

        list0 = new ExprList(objectPool);
        list1 = new ExprList(vector1, objectPool);
        list2 = new ExprList(vector2, objectPool);

        /* ----- Did the lists get made? ----- */
        test("Made list 0", (list0 != null), "Did the lists get made? 0", results);
        test("Made list 1", (list1 != null), "Did the lists get made? 1", results);
        test("Made list 2", (list2 != null), "Did the lists get made? 2", results);

        /* ----- Are these lists identical to themselves? ----- */
        test("ExprList identical 0", list0.sameAs(list0), "Are these lists identical to themselves? 0", results);
        test("ExprList identical 1", list1.sameAs(list1), "Are these lists identical to themselves? 1", results);
        test("ExprList identical 2", list2.sameAs(list2), "Are these lists identical to themselves? 2", results);

        /* ----- Are they different from each other? ----- */
        test("ExprLists different 0-1", !(list0.sameAs(list1)), "Are these lists different from each other? 0",
                results);
        test("ExprLists different 1-2", !(list1.sameAs(list2)), "Are these lists identical from each other? 1",
                results);
        test("ExprLists different 0-2", !(list0.sameAs(list2)), "Are these lists identical from each other? 2",
                results);

        /* ----- Check the size of the ExprLists to make sure they are ok ----- */
        test("ExprList size 0", (list0.size() == 0), "check list size? 0", results);
        test("ExprList size 1", (list1.size() == 1), "check list size? 1", results);
        test("ExprList size 2", (list2.size() == 2), "check list size? 2", results);

        /* ----- Make copies of the ExprLists ----- */
        list0Copy = (ExprList) list0.copy();
        list1Copy = (ExprList) list1.copy();
        list2Copy = (ExprList) list2.copy();

        /* ----- Did the copies get made? ----- */
        test("Made copy of list 0", (list0Copy != null), "Did the copies get made? 0", results);
        test("Made copy of list 1", (list1Copy != null), "Did the copies get made? 1", results);
        test("Made copy of list 2", (list2Copy != null), "Did the copies get made? 2", results);

        /* ----- Are they identical to the originals? ----- */
        test("ExprList self-identity 0", (list0.sameAs(list0Copy)), "Are they identical to the originals? 0", results);
        test("ExprList self-identity 1", (list1.sameAs(list1Copy)), "Are they identical to the originals? 1", results);
        test("ExprList self-identity 2", (list2.sameAs(list2Copy)), "Are they identical to the originals? 2", results);

        /* ----- Test adding and deleting from a list ----- */
        Literal add;
        add = Literal.createReal("2.2", objectPool);

        if (list2Copy != null) {
            list2Copy.insert(add);
            test("Edited list is different", !(list2.sameAs(list2Copy)), "Test adding and deleting from a list 0",
                    results);
            list2Copy.erase(list2Copy.size() - 1);
            test("Twice Edited list is same", (list2.sameAs(list2Copy)), "Test adding and deleting from a list 1",
                    results);
        }

        // Note that we do not delete the Literals that we created, because
        // they should have been deleted when the list was deleted.

        /* ----- Test an ExprList bug that Nate Mueller found ----- */
        ClassAd classad;
        ClassAdParser parser = new ClassAdParser(objectPool);
        MutableBoolean b = new MutableBoolean();
        boolean haveAttribute;
        boolean canEvaluate;
        Value value = new Value(objectPool);

        String listClassadText = "[foo = 3; have_foo = member(foo, {1, 2, 3});]";
        classad = parser.parseClassAd(listClassadText);
        haveAttribute = classad.evaluateAttrBool("have_foo", b);
        test("Can evaluate list in member function", (haveAttribute == true && b.booleanValue() == true),
                "Test an ExprList bug that Nate Mueller found 0", results);

        canEvaluate = classad.evaluateExpr("member(foo, {1, 2, blah, 3})", value);
        test("Can evaluate list in member() outside of ClassAd", canEvaluate == true,
                "Test an ExprList bug that Nate Mueller found 1", results);
        return;
    }

    /*********************************************************************
     * Function: test_value
     * Purpose: Test the Value class.
     *
     * @throws HyracksDataException
     *********************************************************************/
    public static void testValue(Parameters parameters, Results results, ClassAdObjectPool objectPool)
            throws HyracksDataException {
        Value v = new Value(objectPool);
        boolean isExpectedType;
        System.out.println("Testing the Value class...");
        test("New value is undefined", (v.isUndefinedValue()), "test_value 1", results);
        test("New value isn't boolean", !(v.isBooleanValue()), "test_value 2", results);
        test("GetType gives UNDEFINED_VALUE", (v.getType() == ValueType.UNDEFINED_VALUE), "test_value 3", results);

        v.setErrorValue();
        test("Is error value", (v.isErrorValue()), "test_value 4", results);
        test("GetType gives ERROR_VALUE", (v.getType() == ValueType.ERROR_VALUE), "test_value 5", results);

        MutableBoolean b = new MutableBoolean();
        v.setBooleanValue(true);
        isExpectedType = v.isBooleanValue(b);
        test("Value is not undefined", !(v.isUndefinedValue()), "Value is not undefined", results);
        test("Value is boolean", (v.isBooleanValue()), "Value is boolean", results);
        test("Try 2: New value is boolean", (isExpectedType == true), "Try 2: New value is boolean", results);
        test("Boolean is true", (b.booleanValue() == true), "Boolean is true", results);
        test("GetType gives BOOLEAN_VALUE", (v.getType() == ValueType.BOOLEAN_VALUE), "GetType gives BOOLEAN_VALUE",
                results);

        AMutableDouble r = new AMutableDouble(0.0);
        v.setRealValue(1.0);
        isExpectedType = v.isRealValue(r);
        test("Value is real", isExpectedType, results);
        test("Real is 1.0", (r.getDoubleValue() == 1.0), results);
        test("GetType gives REAL_VALUE", (v.getType() == ValueType.REAL_VALUE), results);
        test("Real is a number", v.isNumber(), results);

        AMutableInt64 i = new AMutableInt64(0);
        v.setIntegerValue(1);
        isExpectedType = v.isIntegerValue(i);
        test("Value is integer", isExpectedType, results);
        test("Integer is 1", (i.getLongValue() == 1), results);
        test("GetType gives INTEGER_VALUE", (v.getType() == ValueType.INTEGER_VALUE), results);
        test("Integer is a number", v.isNumber(), results);

        AMutableCharArrayString s = new AMutableCharArrayString();
        v.setStringValue("Robert-Houdin");
        isExpectedType = v.isStringValue(s);
        test("Value is String", isExpectedType, results);
        test("String is 'Robert-Houdin'", (0 == s.compareTo("Robert-Houdin")), results);
        test("GetType gives STRING_VALUE", (v.getType() == ValueType.STRING_VALUE), results);

        ClassAdTime at = new ClassAdTime(10, 36000000);
        v.setAbsoluteTimeValue(at);
        at.setValue(0);
        at.setTimeZone(0);
        isExpectedType = v.isAbsoluteTimeValue(at);
        test("Value is absolute time", isExpectedType, results);
        test("Absolute time is 10, 0", (10 == at.getTime() && 36000000 == at.getOffset()), results);
        test("GetType gives ABSOLUTE_TIME_VALUE", (v.getType() == ValueType.ABSOLUTE_TIME_VALUE), results);

        ClassAdTime rt = new ClassAdTime(10, false);
        v.setRelativeTimeValue(10);
        isExpectedType = v.isRelativeTimeValue(rt);
        test("Value is relative time", isExpectedType, results);
        test("Relative time is 10", (10 == rt.getRelativeTime()), results);
        test("GetType gives RELATIVE_TIME_VALUE", (v.getType() == ValueType.RELATIVE_TIME_VALUE), results);

        ExprList l = new ExprList(objectPool);
        ExprList ll = new ExprList(objectPool);
        v.setListValue(l);
        isExpectedType = v.isListValue(ll);
        test("Value is list value", isExpectedType, results);
        test("List value is correct", l.equals(ll), results);
        test("GetType gives LIST_VALUE", (v.getType() == ValueType.LIST_VALUE), results);

        ExprList sl = new ExprList(true, objectPool);
        ll = new ExprList(true, objectPool);
        v.setListValue(sl);
        isExpectedType = v.isListValue(ll);
        test("Value is list value", isExpectedType, results);
        test("List value is correct", sl.equals(ll), results);
        test("GetType gives SLIST_VALUE", (v.getType() == ValueType.SLIST_VALUE), results);

        ClassAd c = new ClassAd(objectPool);
        c.insertAttr("test_int", 10);
        ClassAd cc = new ClassAd(objectPool);
        v.setClassAdValue(c);
        isExpectedType = v.isClassAdValue(cc);
        test("Value is ClassAd value", isExpectedType, results);
        test("ClassAd value is correct", c.equals(cc), results);
        test("GetType gives CLASSAD_VALUE", (v.getType() == ValueType.CLASSAD_VALUE), results);
        return;
    }

    /*********************************************************************
     * Function: test_utils
     * Purpose: Test utils
     *********************************************************************/
    public static void testUtils(Parameters parameters, Results results) {
        System.out.println("Testing little utilities...");

        test("1800 is not a leap year", !Util.isLeapYear(1800), results);
        test("1900 is not a leap year", !Util.isLeapYear(1900), results);
        test("2000 is a leap year", Util.isLeapYear(2000), results);
        test("2001 is not a leap year", !Util.isLeapYear(2001), results);
        test("2002 is not a leap year", !Util.isLeapYear(2002), results);
        test("2003 is not a leap year", !Util.isLeapYear(2003), results);
        test("2004 is a leap year", Util.isLeapYear(2004), results);

        test("70, 9, 24 . 25469", Util.fixedFromGregorian(70, 9, 24) == 25469, results);
        test("135, 10, 2 . 49217", Util.fixedFromGregorian(135, 10, 2) == 49217, results);
        test("470, 1, 8 . 171307", Util.fixedFromGregorian(470, 1, 8) == 171307, results);
        test("576, 5, 20 . 210155", Util.fixedFromGregorian(576, 5, 20) == 210155, results);
        test("694,  11, 10 . 253427", Util.fixedFromGregorian(694, 11, 10) == 253427, results);
        test("1013,  4, 25 . 369740", Util.fixedFromGregorian(1013, 4, 25) == 369740, results);
        test("1096,  5, 24 . 400085", Util.fixedFromGregorian(1096, 5, 24) == 400085, results);
        test("1190,  3, 23 . 434355", Util.fixedFromGregorian(1190, 3, 23) == 434355, results);
        test("1240,  3, 10 . 452605", Util.fixedFromGregorian(1240, 3, 10) == 452605, results);
        test("1288,  4, 2 . 470160", Util.fixedFromGregorian(1288, 4, 2) == 470160, results);
        test("1298,  4, 27 . 473837", Util.fixedFromGregorian(1298, 4, 27) == 473837, results);
        test("1391,  6, 12 . 507850", Util.fixedFromGregorian(1391, 6, 12) == 507850, results);
        test("1436,  2, 3 . 524156", Util.fixedFromGregorian(1436, 2, 3) == 524156, results);
        test("1492,  4, 9 . 544676", Util.fixedFromGregorian(1492, 4, 9) == 544676, results);
        test("1553,  9, 19 . 567118", Util.fixedFromGregorian(1553, 9, 19) == 567118, results);
        test("1560,  3, 5 . 569477", Util.fixedFromGregorian(1560, 3, 5) == 569477, results);
        test("1648,  6, 10 . 601716", Util.fixedFromGregorian(1648, 6, 10) == 601716, results);
        test("1680,  6, 30 . 613424", Util.fixedFromGregorian(1680, 6, 30) == 613424, results);
        test("1716,  7, 24 . 626596", Util.fixedFromGregorian(1716, 7, 24) == 626596, results);
        test("1768,  6, 19 . 645554", Util.fixedFromGregorian(1768, 6, 19) == 645554, results);
        test("1819,  8, 2 . 664224", Util.fixedFromGregorian(1819, 8, 2) == 664224, results);
        test("1839,  3, 27 . 671401", Util.fixedFromGregorian(1839, 3, 27) == 671401, results);
        test("1903,  4, 19 . 694799", Util.fixedFromGregorian(1903, 4, 19) == 694799, results);
        test("1929,  8, 25 . 704424", Util.fixedFromGregorian(1929, 8, 25) == 704424, results);
        test("1941,  9, 29 . 708842", Util.fixedFromGregorian(1941, 9, 29) == 708842, results);
        test("1943,  4, 19 . 709409", Util.fixedFromGregorian(1943, 4, 19) == 709409, results);
        test("1943,  10, 7 . 709580", Util.fixedFromGregorian(1943, 10, 7) == 709580, results);
        test("1992,  3, 17 . 727274", Util.fixedFromGregorian(1992, 3, 17) == 727274, results);
        test("1996,  2, 25 . 728714", Util.fixedFromGregorian(1996, 2, 25) == 728714, results);
        test("2038,  11, 10 . 744313", Util.fixedFromGregorian(2038, 11, 10) == 744313, results);
        test("2094,  7, 18 . 764652", Util.fixedFromGregorian(2094, 7, 18) == 764652, results);

        AMutableInt32 weekday = new AMutableInt32(0);
        AMutableInt32 yearday = new AMutableInt32(0);
        Util.dayNumbers(2005, 1, 1, weekday, yearday);
        test("Jan 1, 2005.6, 0", weekday.getIntegerValue() == 6 && yearday.getIntegerValue() == 0, results);
        Util.dayNumbers(2005, 1, 2, weekday, yearday);
        test("Jan 2, 2005.6, 1", weekday.getIntegerValue() == 0 && yearday.getIntegerValue() == 1, results);
        Util.dayNumbers(2005, 12, 30, weekday, yearday);
        test("Dec 30, 2005.5, 363", weekday.getIntegerValue() == 5 && yearday.getIntegerValue() == 363, results);
        Util.dayNumbers(2005, 12, 31, weekday, yearday);
        test("Dec 31, 2005.6, 364", weekday.getIntegerValue() == 6 && yearday.getIntegerValue() == 364, results);
        Util.dayNumbers(2004, 12, 31, weekday, yearday);
        test("Dec 31, 2005.5, 365", weekday.getIntegerValue() == 5 && yearday.getIntegerValue() == 365, results);
        return;
    }

    /*********************************************************************
     * Function: print_version
     * Purpose:
     *********************************************************************/
    public static void printVersion() {
        AMutableString classadVersion = new AMutableString(null);
        ClassAd.classAdLibraryVersion(classadVersion);
        System.out.println("ClassAd Unit Tester v" + classadVersion + "\n");
    }
}
