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
package org.apache.asterix.tools.datagen;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.asterix.common.annotations.AutoDataGen;
import org.apache.asterix.common.annotations.DateBetweenYearsDataGen;
import org.apache.asterix.common.annotations.DatetimeAddRandHoursDataGen;
import org.apache.asterix.common.annotations.DatetimeBetweenYearsDataGen;
import org.apache.asterix.common.annotations.FieldIntervalDataGen;
import org.apache.asterix.common.annotations.FieldValFileDataGen;
import org.apache.asterix.common.annotations.FieldValFileSameIndexDataGen;
import org.apache.asterix.common.annotations.IRecordFieldDataGen;
import org.apache.asterix.common.annotations.IRecordTypeAnnotation;
import org.apache.asterix.common.annotations.IRecordTypeAnnotation.Kind;
import org.apache.asterix.common.annotations.InsertRandIntDataGen;
import org.apache.asterix.common.annotations.ListDataGen;
import org.apache.asterix.common.annotations.ListValFileDataGen;
import org.apache.asterix.common.annotations.RecordDataGenAnnotation;
import org.apache.asterix.common.annotations.TypeDataGen;
import org.apache.asterix.common.annotations.UndeclaredFieldsDataGen;
import org.apache.asterix.common.exceptions.ACIDException;
import org.apache.asterix.common.transactions.TxnId;
import org.apache.asterix.lang.aql.parser.AQLParserFactory;
import org.apache.asterix.lang.aql.parser.ParseException;
import org.apache.asterix.lang.common.base.IParser;
import org.apache.asterix.lang.common.base.IParserFactory;
import org.apache.asterix.lang.common.base.Statement;
import org.apache.asterix.metadata.MetadataTransactionContext;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.om.types.ATypeTag;
import org.apache.asterix.om.types.AUnionType;
import org.apache.asterix.om.types.AbstractCollectionType;
import org.apache.asterix.om.types.BuiltinType;
import org.apache.asterix.om.types.IAType;
import org.apache.asterix.om.types.TypeSignature;
import org.apache.asterix.om.utils.NonTaggedFormatUtil;
import org.apache.asterix.test.base.AsterixTestHelper;
import org.apache.asterix.tools.translator.ADGenDmlTranslator;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.algebricks.common.exceptions.NotImplementedException;
import org.apache.hyracks.algebricks.data.utils.WriteValueTools;

public class AdmDataGen {

    class DataGeneratorContext {
        private Map<File, String[]> fileToLoadedDataMap = new HashMap<File, String[]>();

        public Map<File, String[]> getFileToLoadedDataMap() {
            return fileToLoadedDataMap;
        }
    }

    public static void printDate(int year, int month, int day, PrintStream out) throws IOException {
        WriteValueTools.writeInt(year, out);
        out.print("-");
        if (month < 10) {
            out.print("0");
        }
        WriteValueTools.writeInt(month, out);
        out.print("-");
        if (day < 10) {
            out.print("0");
        }
        WriteValueTools.writeInt(day, out);
    }

    abstract class AbstractValueGenerator {
        protected PrintStream out;
        protected DataGeneratorContext ctx;

        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            this.out = out;
            this.ctx = ctx;
        }

        public abstract void generate() throws IOException;
    }

    abstract class RandomValueGenerator extends AbstractValueGenerator {
        protected Random rnd;

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            this.rnd = new Random(51);
        }

    }

    class IntAutoGenerator extends AbstractValueGenerator {

        private final int initValue;
        private int val;

        public IntAutoGenerator(int initValue) {
            this.initValue = initValue;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            val = initValue;
        }

        @Override
        public void generate() throws IOException {
            WriteValueTools.writeInt(val, out);
            val++;
        }

    }

    class LongAutoGenerator extends AbstractValueGenerator {

        private final long initValue;
        private long val;

        public LongAutoGenerator(long initValue) {
            this.initValue = initValue;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            val = initValue;
        }

        @Override
        public void generate() throws IOException {
            WriteValueTools.writeLong(val, out);
            out.print("i64");
            val++;
        }

    }

    class StringFromArrayGenerator extends RandomValueGenerator {
        private final String[][] array;
        private int lastIndex;
        private final String constructor;
        private String[] chosen;

        public StringFromArrayGenerator(String[][] array, String constructor) {
            this.array = array;
            this.constructor = constructor;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            this.lastIndex = -1;
            this.chosen = new String[array.length];
        }

        @Override
        public void generate() throws IOException {
            for (int i = 0; i < array.length; i++) {
                lastIndex = Math.abs(rnd.nextInt()) % array[i].length;
                chosen[i] = array[i][lastIndex];
            }
            if (constructor != null) {
                out.print(constructor);
                out.print("(");
            }
            out.print("\"");
            for (int i = 0; i < chosen.length; i++) {
                out.print(chosen[i]);
            }
            out.print("\"");
            if (constructor != null) {
                out.print(")");
            }
        }

        public int getLastIndex() {
            return lastIndex;
        }
    }

    abstract class AbstractCollectionDataGenerator extends RandomValueGenerator {
        protected final AbstractCollectionType act;
        protected final int min;
        protected final int max;
        protected final String startList;
        protected final String endList;

        public AbstractCollectionDataGenerator(AbstractCollectionType act, int min, int max) {
            this.act = act;
            this.min = min;
            this.max = max;
            if (act.getTypeTag() == ATypeTag.ARRAY) {
                startList = "[";
                endList = "]";
            } else {
                startList = "{{";
                endList = "}}";
            }
        }

    }

    class ListDataGenerator extends AbstractCollectionDataGenerator {

        private AbstractValueGenerator itemGen;

        public ListDataGenerator(AbstractCollectionType act, int min, int max) {
            super(act, min, max);
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            IAType t = act.getItemType();
            if (t.getTypeTag() != ATypeTag.OBJECT) {
                throw new NotImplementedException("list annotation only works with record item types for now.");
            }
            ARecordType rt = (ARecordType) t;
            RecordDataGenAnnotation dga = firstDataGenAnnotation(rt);
            if (dga == null) {
                throw new Exception("No annotation on item type " + t);
            }
            itemGen = new RecordGenerator(rt, dga, null);
            itemGen.init(out, ctx);
        }

        @Override
        public void generate() throws IOException {
            out.print(startList);
            int numItems = rnd.nextInt(max - min + 1) + min;
            for (int i = 0; i < numItems; i++) {
                if (i > 0) {
                    out.print(", ");
                }
                itemGen.generate();
            }
            out.print(endList);
        }

    }

    class ListFromArrayGenerator extends AbstractCollectionDataGenerator {

        private final String[] array;
        private String constructor;

        public ListFromArrayGenerator(AbstractCollectionType act, String[] array, int min, int max) {
            super(act, min, max);
            this.array = array;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            if (act.getItemType().getTypeTag() == ATypeTag.STRING) {
                constructor = null;
            } else {
                constructor = getConstructor(act.getItemType());
            }
        }

        @Override
        public void generate() throws IOException {
            out.print(startList);
            int numItems = rnd.nextInt(max - min + 1) + min;
            for (int i = 0; i < numItems; i++) {
                if (i > 0) {
                    out.print(", ");
                }
                int c = Math.abs(rnd.nextInt()) % array.length;
                if (constructor != null) {
                    out.print(constructor);
                    out.print("(");
                }
                out.print("\"");
                out.print(array[c]);
                out.print("\"");
                if (constructor != null) {
                    out.print(")");
                }
            }
            out.print(endList);
        }

    }

    class StringFromArraySameIndexGenerator extends AbstractValueGenerator {
        private final String[] array;
        private final StringFromArrayGenerator sfag;
        private final String constructor;

        public StringFromArraySameIndexGenerator(String[] array, StringFromArrayGenerator sfag, String constructor) {
            this.array = array;
            this.sfag = sfag;
            this.constructor = constructor;
        }

        @Override
        public void generate() throws IOException {
            String val = array[sfag.getLastIndex()];
            if (constructor != null) {
                out.print(constructor);
                out.print("(");
            }
            out.print("\"");
            out.print(val);
            out.print("\"");
            if (constructor != null) {
                out.print(")");
            }
        }
    }

    class IntIntervalGenerator extends RandomValueGenerator {

        private final int min;
        private final int max;
        private final String prefix;
        private final String suffix;

        public IntIntervalGenerator(int min, int max, String prefix, String suffix) {
            this.min = min;
            this.max = max;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public void generate() throws IOException {
            int v = Math.abs(rnd.nextInt() % (max - min + 1)) + min;
            if (prefix != null) {
                out.print(prefix);
            }
            WriteValueTools.writeInt(v, out);
            if (suffix != null) {
                out.print(suffix);
            }
        }

    }

    class LongIntervalGenerator extends RandomValueGenerator {

        private final long min;
        private final long max;
        private final String prefix;
        private final String suffix;

        public LongIntervalGenerator(long min, long max, String prefix, String suffix) {
            this.min = min;
            this.max = max;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public void generate() throws IOException {
            long v = Math.abs(rnd.nextLong() % (max - min + 1)) + min;
            if (prefix != null) {
                out.print(prefix);
            }
            WriteValueTools.writeLong(v, out);
            if (suffix != null) {
                out.print(suffix);
            }
        }

    }

    class DoubleIntervalGenerator extends RandomValueGenerator {

        private final double min;
        private final double max;
        private final String prefix;
        private final String suffix;

        public DoubleIntervalGenerator(double min, double max, String prefix, String suffix) {
            this.min = min;
            this.max = max;
            this.prefix = prefix;
            this.suffix = suffix;
        }

        @Override
        public void generate() throws IOException {
            double v = Math.abs(rnd.nextDouble() % (max - min)) + min;
            if (prefix != null) {
                out.print(prefix);
            }
            out.print(v);
            if (suffix != null) {
                out.print(suffix);
            }
        }

    }

    class InsertRandIntGenerator extends RandomValueGenerator {

        private final String str1;
        private final String str2;

        public InsertRandIntGenerator(String str1, String str2) {
            this.str1 = "\"" + str1;
            this.str2 = str2 + "\"";
        }

        @Override
        public void generate() throws IOException {
            int v = Math.abs(rnd.nextInt());
            out.print(str1);
            WriteValueTools.writeInt(v, out);
            out.print(str2);
        }

    }

    interface AccessibleDateGenerator {
        public int getYear();

        public int getMonth();

        public int getDay();
    }

    abstract class DateGenerator extends RandomValueGenerator implements AccessibleDateGenerator {

        private int year;
        private int month;
        private int day;

        protected void generateDate(int minYear, int maxYear) throws IOException {
            year = rnd.nextInt(maxYear - minYear + 1) + minYear;
            month = Math.abs(rnd.nextInt()) % 12 + 1;
            day = Math.abs(rnd.nextInt()) % 28 + 1;
            printDate(year, month, day, out);
        }

        @Override
        public int getYear() {
            return year;
        }

        @Override
        public int getMonth() {
            return month;
        }

        @Override
        public int getDay() {
            return day;
        }
    }

    class DateBetweenYearsGenerator extends DateGenerator {

        private final int minYear;
        private final int maxYear;

        public DateBetweenYearsGenerator(int minYear, int maxYear) {
            this.minYear = minYear;
            this.maxYear = maxYear;
        }

        @Override
        public void generate() throws IOException {
            out.print("date(\"");
            generateDate(minYear, maxYear);
            out.print("\")");
        }

    }

    interface AccessibleDatetimeGenerator extends AccessibleDateGenerator {
        public int getHour();

        public int getMinute();

        public int getSecond();
    }

    class DatetimeBetweenYearsGenerator extends DateGenerator implements AccessibleDatetimeGenerator {
        private final int minYear;
        private final int maxYear;
        private int hour;

        public DatetimeBetweenYearsGenerator(int minYear, int maxYear) {
            this.minYear = minYear;
            this.maxYear = maxYear;
        }

        @Override
        public void generate() throws IOException {
            out.print("datetime(\"");
            generateDate(minYear, maxYear);
            out.print("T");
            hour = rnd.nextInt(24);
            if (hour < 10) {
                out.print("0");
            }
            WriteValueTools.writeInt(hour, out);
            out.print(":00:00\")");
        }

        @Override
        public int getHour() {
            return hour;
        }

        @Override
        public int getMinute() {
            return 0;
        }

        @Override
        public int getSecond() {
            return 0;
        }

    }

    class DatetimeAddRandHoursGenerator extends RandomValueGenerator {

        private final int minHours;
        private final int maxHours;
        private final AccessibleDatetimeGenerator adtg;

        public DatetimeAddRandHoursGenerator(int minHours, int maxHours, AccessibleDatetimeGenerator adtg) {
            this.minHours = minHours;
            this.maxHours = maxHours;
            this.adtg = adtg;
        }

        @Override
        public void generate() throws IOException {
            int addHours = rnd.nextInt(maxHours - minHours + 1) + minHours;

            out.print("datetime(\"");
            printDate(adtg.getYear(), adtg.getMonth(), adtg.getDay(), out);
            out.print("T");
            int h = adtg.getHour() + addHours;
            if (h > 23) {
                h = 23;
            }
            if (h < 10) {
                out.print("0");
            }
            WriteValueTools.writeInt(h, out);
            out.print(":");
            int m = adtg.getMinute();
            if (m < 10) {
                out.print("0");
            }
            WriteValueTools.writeInt(m, out);
            out.print(":");
            int s = adtg.getSecond();
            if (s < 10) {
                out.print("0");
            }
            WriteValueTools.writeInt(s, out);
            out.print("\")");
        }

    }

    class GenFieldsIntGenerator extends RandomValueGenerator {
        private final int minFields;
        private final int maxFields;
        private final String prefix;
        private final int startIndex;
        private String[] fieldNames;
        private int[] id;
        private int[] x;

        public GenFieldsIntGenerator(int startIndex, int minFields, int maxFields, String prefix) {
            this.startIndex = startIndex;
            this.minFields = minFields;
            this.maxFields = maxFields;
            this.prefix = prefix;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            fieldNames = new String[maxFields];
            for (int i = 0; i < maxFields; i++) {
                fieldNames[i] = prefix + "_" + i;
            }
            id = new int[maxFields];
            for (int i = 0; i < maxFields; i++) {
                id[i] = i;
            }
            x = new int[maxFields];
        }

        @Override
        public void generate() throws IOException {
            int numFields = rnd.nextInt(maxFields - minFields + 1) + minFields;
            System.arraycopy(id, 0, x, 0, maxFields);
            int n = numFields;
            boolean first = startIndex < 1;
            while (n > 0) {
                int c = rnd.nextInt(n);
                if (first) {
                    first = false;
                } else {
                    out.print(",");
                }
                out.print(" \"");
                out.print(fieldNames[x[c]]);
                out.print("\": ");
                WriteValueTools.writeInt(Math.abs(rnd.nextInt()), out);
                x[c] = x[n - 1];
                n--;
            }
        }
    }

    class RecordGenerator extends RandomValueGenerator {

        private AbstractValueGenerator[] declaredFieldsGenerators;
        private boolean[] nullable;
        private AbstractValueGenerator undeclaredFieldsGenerator;
        private final ARecordType recType;
        private final RecordDataGenAnnotation annot;
        private final String printAfter;

        public RecordGenerator(ARecordType type, RecordDataGenAnnotation annot, String printAfter) {
            this.recType = type;
            this.annot = annot;
            this.printAfter = printAfter;
        }

        @Override
        public void init(PrintStream out, DataGeneratorContext ctx) throws Exception {
            super.init(out, ctx);
            if (declaredFieldsGenerators == null) {
                int m = annot.getDeclaredFieldsDatagen().length;
                declaredFieldsGenerators = new AbstractValueGenerator[m];
                nullable = new boolean[m];
                for (int i = 0; i < m; i++) {
                    IAType ti = recType.getFieldTypes()[i];
                    if (NonTaggedFormatUtil.isOptional(ti)) {
                        ti = ((AUnionType) ti).getActualType();
                        nullable[i] = true;
                    }
                    IRecordFieldDataGen rfdg = annot.getDeclaredFieldsDatagen()[i];
                    if (rfdg == null) {
                        if (ti.getTypeTag() == ATypeTag.OBJECT) {
                            ARecordType rt = (ARecordType) ti;
                            RecordDataGenAnnotation dga = null;
                            for (IRecordTypeAnnotation annot : rt.getAnnotations()) {
                                if (annot.getKind() == Kind.RECORD_DATA_GEN) {
                                    dga = (RecordDataGenAnnotation) annot;
                                    break;
                                }
                            }
                            if (dga != null) {
                                declaredFieldsGenerators[i] = new RecordGenerator(rt, dga, null);
                                continue;
                            }
                        }
                        if (declaredFieldsGenerators[i] == null) {
                            throw new Exception("No data generator annotation for field " + recType.getFieldNames()[i]
                                    + " in type " + recType);
                        }
                    }
                    switch (rfdg.getKind()) {
                        case VALFILE: {
                            FieldValFileDataGen vf = (FieldValFileDataGen) rfdg;
                            int numFiles = vf.getFiles().length;
                            String[][] a = new String[numFiles][];
                            for (int k = 0; k < numFiles; k++) {
                                File f = vf.getFiles()[k];
                                a[k] = ctx.getFileToLoadedDataMap().get(f);
                                if (a[k] == null) {
                                    a[k] = readFileAsStringArray(f);
                                    ctx.getFileToLoadedDataMap().put(f, a[k]);
                                }
                            }
                            String constructor;
                            if (ti.getTypeTag() == ATypeTag.STRING) {
                                constructor = null;
                            } else {
                                constructor = getConstructor(ti);
                            }
                            declaredFieldsGenerators[i] = new StringFromArrayGenerator(a, constructor);
                            break;
                        }
                        case LISTVALFILE: {
                            ListValFileDataGen lvf = (ListValFileDataGen) rfdg;
                            String[] a = ctx.getFileToLoadedDataMap().get(lvf.getFile());
                            if (a == null) {
                                a = readFileAsStringArray(lvf.getFile());
                                ctx.getFileToLoadedDataMap().put(lvf.getFile(), a);
                            }
                            if (ti.getTypeTag() != ATypeTag.ARRAY && ti.getTypeTag() != ATypeTag.MULTISET) {
                                throw new Exception(
                                        "list-val-file annotation cannot be used for field of type " + ti.getTypeTag());
                            }
                            AbstractCollectionType act = (AbstractCollectionType) ti;
                            declaredFieldsGenerators[i] =
                                    new ListFromArrayGenerator(act, a, lvf.getMin(), lvf.getMax());
                            break;
                        }
                        case VALFILESAMEINDEX: {
                            FieldValFileSameIndexDataGen vfsi = (FieldValFileSameIndexDataGen) rfdg;
                            String[] a = ctx.getFileToLoadedDataMap().get(vfsi.getFile());
                            if (a == null) {
                                a = readFileAsStringArray(vfsi.getFile());
                                ctx.getFileToLoadedDataMap().put(vfsi.getFile(), a);
                            }
                            StringFromArrayGenerator sfag = null;
                            for (int j = 0; j < i; j++) {
                                if (recType.getFieldNames()[j].equals(vfsi.getSameAsField())) {
                                    if (declaredFieldsGenerators[j] instanceof StringFromArrayGenerator) {
                                        sfag = (StringFromArrayGenerator) declaredFieldsGenerators[j];
                                        break;
                                    } else {
                                        throw new Exception("Data generator for field " + recType.getFieldNames()[j]
                                                + " is not based on values from a text file, as required by generator for field "
                                                + recType.getFieldNames()[i]);
                                    }
                                }
                            }
                            if (sfag == null) {
                                throw new Exception("Couldn't find field " + vfsi.getSameAsField() + " before field "
                                        + recType.getFieldNames()[i]);
                            }
                            String constructor;
                            if (ti.getTypeTag() == ATypeTag.STRING) {
                                constructor = null;
                            } else {
                                constructor = getConstructor(ti);
                            }
                            declaredFieldsGenerators[i] = new StringFromArraySameIndexGenerator(a, sfag, constructor);
                            break;
                        }
                        case INTERVAL: {
                            FieldIntervalDataGen fi = (FieldIntervalDataGen) rfdg;
                            String prefix = null;
                            String suffix = null;
                            if (ti.getTypeTag() == ATypeTag.STRING) {
                                prefix = "\"";
                                suffix = "\"";
                            }
                            switch (fi.getValueType()) {
                                case INT: {
                                    declaredFieldsGenerators[i] =
                                            new IntIntervalGenerator(Integer.parseInt(fi.getMin()),
                                                    Integer.parseInt(fi.getMax()), prefix, suffix);
                                    break;
                                }
                                case LONG: {
                                    declaredFieldsGenerators[i] = new LongIntervalGenerator(Long.parseLong(fi.getMin()),
                                            Long.parseLong(fi.getMax()), prefix, suffix);
                                    break;
                                }
                                case DOUBLE: {
                                    declaredFieldsGenerators[i] =
                                            new DoubleIntervalGenerator(Double.parseDouble(fi.getMin()),
                                                    Double.parseDouble(fi.getMax()), prefix, suffix);
                                    break;
                                }
                                default: {
                                    throw new NotImplementedException();
                                }
                            }
                            break;
                        }
                        case INSERTRANDINT: {
                            InsertRandIntDataGen iri = (InsertRandIntDataGen) rfdg;
                            declaredFieldsGenerators[i] = new InsertRandIntGenerator(iri.getStr1(), iri.getStr2());
                            break;
                        }
                        case LIST: {
                            ListDataGen l = (ListDataGen) rfdg;
                            if (ti.getTypeTag() != ATypeTag.ARRAY && ti.getTypeTag() != ATypeTag.MULTISET) {
                                throw new Exception(
                                        "list-val-file annotation cannot be used for field of type " + ti.getTypeTag());
                            }
                            AbstractCollectionType act = (AbstractCollectionType) ti;
                            declaredFieldsGenerators[i] = new ListDataGenerator(act, l.getMin(), l.getMax());
                            break;
                        }
                        case DATEBETWEENYEARS: {
                            DateBetweenYearsDataGen dby = (DateBetweenYearsDataGen) rfdg;
                            declaredFieldsGenerators[i] =
                                    new DateBetweenYearsGenerator(dby.getMinYear(), dby.getMaxYear());
                            break;
                        }
                        case DATETIMEBETWEENYEARS: {
                            DatetimeBetweenYearsDataGen dtby = (DatetimeBetweenYearsDataGen) rfdg;
                            declaredFieldsGenerators[i] =
                                    new DatetimeBetweenYearsGenerator(dtby.getMinYear(), dtby.getMaxYear());
                            break;
                        }
                        case DATETIMEADDRANDHOURS: {
                            DatetimeAddRandHoursDataGen dtarh = (DatetimeAddRandHoursDataGen) rfdg;
                            AccessibleDatetimeGenerator adtg = null;
                            for (int j = 0; j < i; j++) {
                                if (recType.getFieldNames()[j].equals(dtarh.getAddToField())) {
                                    if (declaredFieldsGenerators[j] instanceof AccessibleDatetimeGenerator) {
                                        adtg = (AccessibleDatetimeGenerator) declaredFieldsGenerators[j];
                                        break;
                                    } else {
                                        throw new Exception("Data generator for field " + recType.getFieldNames()[j]
                                                + " is not based on accessible datetime values, as required by generator for field "
                                                + recType.getFieldNames()[i]);
                                    }
                                }
                            }
                            if (adtg == null) {
                                throw new Exception("Couldn't find field " + dtarh.getAddToField() + " before field "
                                        + recType.getFieldNames()[i]);
                            }
                            declaredFieldsGenerators[i] =
                                    new DatetimeAddRandHoursGenerator(dtarh.getMinHour(), dtarh.getMaxHour(), adtg);
                            break;
                        }
                        case AUTO: {
                            AutoDataGen auto = (AutoDataGen) rfdg;
                            switch (ti.getTypeTag()) {
                                case INTEGER: {
                                    declaredFieldsGenerators[i] =
                                            new IntAutoGenerator(Integer.parseInt(auto.getInitValueStr()));
                                    break;
                                }
                                case BIGINT: {
                                    declaredFieldsGenerators[i] =
                                            new LongAutoGenerator(Long.parseLong(auto.getInitValueStr()));
                                    break;
                                }
                                default: {
                                    throw new IllegalStateException(rfdg.getKind()
                                            + " annotation is not implemented for type " + ti.getTypeTag());
                                }
                            }
                            break;
                        }
                        default: {
                            throw new NotImplementedException(rfdg.getKind() + " is not implemented");
                        }
                    }
                }
            }
            for (int i = 0; i < declaredFieldsGenerators.length; i++) {
                declaredFieldsGenerators[i].init(out, ctx);
            }
            if (undeclaredFieldsGenerator == null) {
                UndeclaredFieldsDataGen ufdg = annot.getUndeclaredFieldsDataGen();
                if (ufdg != null) {
                    if (!recType.isOpen()) {
                        throw new Exception("Cannot generate undeclared fields for closed type " + recType);
                    }
                    undeclaredFieldsGenerator =
                            new GenFieldsIntGenerator(declaredFieldsGenerators.length, ufdg.getMinUndeclaredFields(),
                                    ufdg.getMaxUndeclaredFields(), ufdg.getUndeclaredFieldsPrefix());
                }
            }
            if (undeclaredFieldsGenerator != null) {
                undeclaredFieldsGenerator.init(out, ctx);
            }
        }

        @Override
        public void generate() throws IOException {
            out.print("{");
            boolean first = true;
            for (int i = 0; i < declaredFieldsGenerators.length; i++) {
                boolean pick;
                if (nullable[i]) {
                    pick = rnd.nextBoolean();
                } else {
                    pick = true;
                }
                if (pick) {
                    if (first) {
                        first = false;
                    } else {
                        out.print(",");
                    }
                    out.print(" \"");
                    out.print(this.recType.getFieldNames()[i]);
                    out.print("\": ");
                    declaredFieldsGenerators[i].generate();
                }
            }
            if (undeclaredFieldsGenerator != null) {
                undeclaredFieldsGenerator.generate();
            }
            out.print(" }");
            if (printAfter != null) {
                out.print(printAfter);
            }
        }

    }

    private final File schemaFile;
    private final File outputDir;
    private Map<TypeSignature, IAType> typeMap;
    private Map<TypeSignature, TypeDataGen> typeAnnotMap;
    private DataGeneratorContext dgCtx;
    private final IParserFactory parserFactory = new AQLParserFactory();

    public AdmDataGen(File schemaFile, File outputDir) {
        this.schemaFile = schemaFile;
        this.outputDir = outputDir;
    }

    public void init() throws IOException, ParseException, ACIDException, AlgebricksException {
        FileReader aql = new FileReader(schemaFile);
        IParser parser = parserFactory.createParser(aql);
        List<Statement> statements = parser.parse();
        aql.close();
        // TODO: Need to fix how to use transactions here.
        MetadataTransactionContext mdTxnCtx = new MetadataTransactionContext(new TxnId(-1));
        ADGenDmlTranslator dmlt = new ADGenDmlTranslator(mdTxnCtx, statements);
        dmlt.translate();
        typeMap = dmlt.getTypeMap();
        typeAnnotMap = dmlt.getTypeDataGenMap();
        dgCtx = new DataGeneratorContext();
    }

    public void dataGen() throws Exception {
        for (Map.Entry<TypeSignature, IAType> me : typeMap.entrySet()) {
            TypeSignature tn = me.getKey();
            TypeDataGen tdg = typeAnnotMap.get(tn);
            if (tdg.isDataGen()) {
                IAType t = me.getValue();

                if (t.getTypeTag() != ATypeTag.OBJECT) {
                    throw new NotImplementedException();
                }
                ARecordType rt = (ARecordType) t;
                RecordDataGenAnnotation dga = firstDataGenAnnotation(rt);
                if (dga == null) {
                    throw new Exception("No data generator annotations for type " + tn);
                }
                File outFile = new File(outputDir + File.separator + tdg.getOutputFileName());
                PrintStream outStream = new PrintStream(new BufferedOutputStream(new FileOutputStream(outFile)));
                RecordGenerator rg = new RecordGenerator(rt, dga, "\n");
                rg.init(outStream, dgCtx);
                for (long i = 0; i < tdg.getNumValues(); i++) {
                    rg.generate();
                }
                outStream.close();
            }
        }
    }

    private static RecordDataGenAnnotation firstDataGenAnnotation(ARecordType rt) {
        for (IRecordTypeAnnotation annot : rt.getAnnotations()) {
            if (annot.getKind() == Kind.RECORD_DATA_GEN) {
                return (RecordDataGenAnnotation) annot;
            }
        }
        return null;
    }

    private static String[] readFileAsStringArray(File file) throws IOException {
        return AsterixTestHelper.readTestListFile(file).toArray(new String[0]);
    }

    private static String getConstructor(IAType t) throws Exception {
        if (t instanceof BuiltinType) {
            String s = ((BuiltinType) t).getConstructor();
            if (s == null) {
                throw new Exception("Type " + t + " has no constructors.");
            }
            return s;
        } else {
            throw new Exception("No string constructor for type " + t);
        }
    }

}
