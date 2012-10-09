package edu.uci.ics.hyracks.algebricks.core.algebra.expressions;

public class IndexedNLJoinExpressionAnnotation implements IExpressionAnnotation {

    public static final String INDEXED_NL_JOIN_ANNOTATION_KEY = "indexnl";
    public static final IndexedNLJoinExpressionAnnotation INSTANCE = new IndexedNLJoinExpressionAnnotation();

    private Object object;

    @Override
    public Object getObject() {
        return object;
    }

    @Override
    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public IExpressionAnnotation copy() {
        IndexedNLJoinExpressionAnnotation clone = new IndexedNLJoinExpressionAnnotation();
        clone.setObject(object);
        return clone;
    }
}
