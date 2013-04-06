package edu.uci.ics.asterix.aql.expression;

import java.io.StringReader;
import java.util.List;

import edu.uci.ics.asterix.aql.base.Statement;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlExpressionVisitor;
import edu.uci.ics.asterix.aql.expression.visitor.IAqlVisitorWithVoidReturn;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.common.functions.FunctionSignature;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.FeedDatasetDetails;
import edu.uci.ics.asterix.metadata.entities.Function;

public class BeginFeedStatement implements Statement {

    private final Identifier dataverseName;
    private final Identifier datasetName;
    private Query query;
    private int varCounter;

    public BeginFeedStatement(Identifier dataverseName, Identifier datasetName, int varCounter) {
        this.dataverseName = dataverseName;
        this.datasetName = datasetName;
        this.varCounter = varCounter;
    }

    public void initialize(MetadataTransactionContext mdTxnCtx, Dataset dataset) throws MetadataException {
        query = new Query();
        FeedDatasetDetails feedDetails = (FeedDatasetDetails) dataset.getDatasetDetails();
        String functionName = feedDetails.getFunction() == null ? null : feedDetails.getFunction().getName();
        StringBuilder builder = new StringBuilder();
        builder.append("insert into dataset " + datasetName + " ");

        if (functionName == null) {
            builder.append(" (" + " for $x in feed-ingest ('" + datasetName + "') ");
            builder.append(" return $x");
        } else {
            int arity = feedDetails.getFunction().getArity();
            FunctionSignature signature = new FunctionSignature(dataset.getDataverseName(), functionName, arity);
            Function function = MetadataManager.INSTANCE.getFunction(mdTxnCtx, signature);
            if (function == null) {
                throw new MetadataException(" Unknown function " + feedDetails.getFunction());
            }
            if (function.getLanguage().equalsIgnoreCase(Function.LANGUAGE_AQL)) {
                String param = function.getParams().get(0);
                builder.append(" (" + " for" + " " + param + " in feed-ingest ('" + datasetName + "') ");
                builder.append(" let $y:=(" + function.getFunctionBody() + ")" + " return $y");
            } else {
                builder.append(" (" + " for $x in feed-ingest ('" + datasetName + "') ");
                builder.append(" let $y:=" + function.getName() + "(" + "$x" + ")");
                builder.append(" return $y");
            }

        }
        builder.append(")");
        builder.append(";");
        AQLParser parser = new AQLParser(new StringReader(builder.toString()));

        List<Statement> statements;
        try {
            statements = parser.Statement();
            query = ((InsertStatement) statements.get(0)).getQuery();
        } catch (ParseException pe) {
            throw new MetadataException(pe);
        }

    }

    public Identifier getDataverseName() {
        return dataverseName;
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
