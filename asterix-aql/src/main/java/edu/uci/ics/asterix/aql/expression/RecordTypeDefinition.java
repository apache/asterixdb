package edu.uci.ics.asterix.aql.expression;

import java.util.ArrayList;

import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.common.annotations.IRecordFieldDataGen;
import edu.uci.ics.asterix.common.annotations.UndeclaredFieldsDataGen;
import edu.uci.ics.asterix.common.exceptions.AsterixException;

public class RecordTypeDefinition extends TypeExpression {

    public enum RecordKind {
        OPEN,
        CLOSED
    }

    private ArrayList<String> fieldNames;
    private ArrayList<TypeExpression> fieldTypes;
    private ArrayList<IRecordFieldDataGen> fieldDataGen;
    private ArrayList<Boolean> nullableFields;
    private RecordKind recordKind;
    private UndeclaredFieldsDataGen undeclaredFieldsDataGen;

    public RecordTypeDefinition() {
        fieldNames = new ArrayList<String>();
        fieldTypes = new ArrayList<TypeExpression>();
        nullableFields = new ArrayList<Boolean>();
        fieldDataGen = new ArrayList<IRecordFieldDataGen>();
    }

    @Override
    public TypeExprKind getTypeKind() {
        return TypeExprKind.RECORD;
    }

    public void addField(String name, TypeExpression type, Boolean nullable, IRecordFieldDataGen fldDataGen) {
        fieldNames.add(name);
        fieldTypes.add(type);
        nullableFields.add(nullable);
        fieldDataGen.add(fldDataGen);
    }

    public void addField(String name, TypeExpression type, Boolean nullable) {
        fieldNames.add(name);
        fieldTypes.add(type);
        nullableFields.add(nullable);
    }

    public ArrayList<String> getFieldNames() {
        return fieldNames;
    }

    public ArrayList<TypeExpression> getFieldTypes() {
        return fieldTypes;
    }

    public ArrayList<Boolean> getNullableFields() {
        return nullableFields;
    }

    public ArrayList<IRecordFieldDataGen> getFieldDataGen() {
        return fieldDataGen;
    }

    public RecordKind getRecordKind() {
        return recordKind;
    }

    public void setRecordKind(RecordKind recordKind) {
        this.recordKind = recordKind;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitRecordTypeDefiniton(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

    public void setUndeclaredFieldsDataGen(UndeclaredFieldsDataGen undeclaredFieldsDataGen) {
        this.undeclaredFieldsDataGen = undeclaredFieldsDataGen;
    }

    public UndeclaredFieldsDataGen getUndeclaredFieldsDataGen() {
        return undeclaredFieldsDataGen;
    }

}
