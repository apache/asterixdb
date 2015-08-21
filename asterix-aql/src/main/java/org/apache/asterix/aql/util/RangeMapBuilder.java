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
package edu.uci.ics.asterix.aql.util;

import java.io.DataOutput;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Expression;
import edu.uci.ics.asterix.aql.base.Expression.Kind;
import edu.uci.ics.asterix.aql.base.Literal;
import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.ListConstructor;
import edu.uci.ics.asterix.aql.expression.LiteralExpr;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.literal.DoubleLiteral;
import edu.uci.ics.asterix.aql.literal.FloatLiteral;
import edu.uci.ics.asterix.aql.literal.IntegerLiteral;
import edu.uci.ics.asterix.aql.literal.LongIntegerLiteral;
import edu.uci.ics.asterix.aql.literal.StringLiteral;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableDouble;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.base.AMutableInt32;
import edu.uci.ics.asterix.om.base.AMutableInt64;
import edu.uci.ics.asterix.om.base.AMutableString;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.partition.range.IRangeMap;
import edu.uci.ics.hyracks.dataflow.common.data.partition.range.RangeMap;

public abstract class RangeMapBuilder {

    public static IRangeMap parseHint(Object hint) throws ParseException {
        ArrayBackedValueStorage abvs = new ArrayBackedValueStorage();
        DataOutput out = abvs.getDataOutput();;
        abvs.reset();

        AQLParser parser = new AQLParser((String) hint);
        List<Statement> hintStatements = parser.parse();
        if (hintStatements.size() != 1) {
            throw new ParseException("Only one range statement is allowed for the range hint.");
        }

        // Translate the query into a Range Map
        if (hintStatements.get(0).getKind() != Statement.Kind.QUERY) {
            throw new ParseException("Not a proper query for the range hint.");
        }
        Query q = (Query) hintStatements.get(0);

        if (q.getBody().getKind() != Kind.LIST_CONSTRUCTOR_EXPRESSION) {
            throw new ParseException("The range hint must be a list.");
        }
        List<Expression> el = (List<Expression>) ((ListConstructor) q.getBody()).getExprList();
        int offsets[] = new int[el.size()];

        // Loop over list of literals
        for (int i = 0; i < el.size(); ++i) {
            Expression item = el.get(i);
            if (item.getKind() == Kind.LITERAL_EXPRESSION) {
                parseLiteralToBytes(item, out);
                offsets[i] = abvs.getLength();
            }
            // TODO Add support for composite fields.
        }

        return new RangeMap(1, abvs.getByteArray(), offsets);
    }

    @SuppressWarnings("unchecked")
    private static void parseLiteralToBytes(Expression item, DataOutput out) throws ParseException {
        AMutableDouble aDouble = new AMutableDouble(0);
        AMutableFloat aFloat = new AMutableFloat(0);
        AMutableInt64 aInt64 = new AMutableInt64(0);
        AMutableInt32 aInt32 = new AMutableInt32(0);
        AMutableString aString = new AMutableString("");
        @SuppressWarnings("rawtypes")
        ISerializerDeserializer serde;

        Literal l = (Literal) ((LiteralExpr) item).getValue();
        try {
            switch (l.getLiteralType()) {
                case DOUBLE:
                    DoubleLiteral dl = (DoubleLiteral) l;
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ADOUBLE);
                    aDouble.setValue(dl.getValue());
                    serde.serialize(aDouble, out);
                    break;
                case FLOAT:
                    FloatLiteral fl = (FloatLiteral) l;
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AFLOAT);
                    aFloat.setValue(fl.getValue());
                    serde.serialize(aFloat, out);
                    break;
                case INTEGER:
                    IntegerLiteral il = (IntegerLiteral) l;
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32);
                    aInt32.setValue(il.getValue());
                    serde.serialize(aInt32, out);
                    break;
                case LONG:
                    LongIntegerLiteral lil = (LongIntegerLiteral) l;
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT64);
                    aInt64.setValue(lil.getValue());
                    serde.serialize(aInt64, out);
                    break;
                case STRING:
                    StringLiteral sl = (StringLiteral) l;
                    serde = AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING);
                    aString.setValue(sl.getValue());
                    serde.serialize(aString, out);
                    break;
                default:
                    throw new NotImplementedException("The range map builder has not been implemented for "
                            + item.getKind() + " type of expressions.");
            }
        } catch (HyracksDataException e) {
            throw new ParseException(e.getMessage());
        }
    }

    public static void verifyRangeOrder(IRangeMap rangeMap, boolean ascending) throws AsterixException {
        // TODO Add support for composite fields.
        int fieldIndex = 0;
        int fieldType = rangeMap.getTag(0, 0);
        AqlBinaryComparatorFactoryProvider comparatorFactory = AqlBinaryComparatorFactoryProvider.INSTANCE;
        IBinaryComparatorFactory bcf = comparatorFactory.getBinaryComparatorFactory(
                ATypeTag.VALUE_TYPE_MAPPING[fieldType], ascending);
        IBinaryComparator comparator = bcf.createBinaryComparator();
        int c = 0;
        for (int split = 1; split < rangeMap.getSplitCount(); ++split) {
            if (fieldType != rangeMap.getTag(fieldIndex, split)) {
                throw new AsterixException("Range field contains more than a single type of items (" + fieldType
                        + " and " + rangeMap.getTag(fieldIndex, split) + ").");
            }
            int previousSplit = split - 1;
            try {
                c = comparator.compare(rangeMap.getByteArray(fieldIndex, previousSplit),
                        rangeMap.getStartOffset(fieldIndex, previousSplit),
                        rangeMap.getLength(fieldIndex, previousSplit), rangeMap.getByteArray(fieldIndex, split),
                        rangeMap.getStartOffset(fieldIndex, split), rangeMap.getLength(fieldIndex, split));
            } catch (HyracksDataException e) {
                throw new AsterixException(e);
            }
            if (c >= 0) {
                throw new AsterixException("Range fields are not in sorted order.");
            }
        }
    }
}
