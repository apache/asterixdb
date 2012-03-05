package edu.uci.ics.asterix.aql.expression;

import java.io.StringReader;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;

public class BeginFeedStatement implements Statement {

    private Identifier datasetName;
    private Query query;
    private int varCounter;

    public BeginFeedStatement(Identifier datasetName, int varCounter) {
        this.datasetName = datasetName;
        this.varCounter = varCounter;
    }
    
    public void initialize(FeedDatasetDetails feedDetails){
        query = new Query();
        String functionName = feedDetails.getFunctionIdentifier();
        String stmt;
        if(functionName == null){
         stmt = "insert into dataset " + datasetName + " (" + " for $x in feed-ingest ('" + datasetName + "')"
                + " return $x" + " );";
        } else {
           stmt = "insert into dataset " + datasetName + " (" + " for $x in feed-ingest ('" + datasetName + "')"
           + " return " + functionName + "(" + "$x" + ")" + ");";
        }
        AQLParser parser = new AQLParser(new StringReader(stmt));
        try {
            query = (Query) parser.Statement();
        } catch (ParseException pe) {
            throw new RuntimeException(pe);
        }

        query = ((InsertStatement) query.getPrologDeclList().get(0)).getQuery();
    }

    public Identifier getDatasetName() {
        return datasetName;
    }

    public Query getQuery() {
        return query;
    }

    public int getVarCounter() {
        return varCounter;
    }

    @Override
    public Kind getKind() {
        return Kind.BEGIN_FEED;
    }

    @Override
    public <R, T> R accept(IAqlExpressionVisitor<R, T> visitor, T arg) throws AsterixException {
        return visitor.visitBeginFeedStatement(this, arg);
    }

    @Override
    public <T> void accept(IAqlVisitorWithVoidReturn<T> visitor, T arg) throws AsterixException {
        visitor.visit(this, arg);
    }

}
