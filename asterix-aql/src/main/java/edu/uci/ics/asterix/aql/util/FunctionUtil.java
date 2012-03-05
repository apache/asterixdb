package edu.uci.ics.asterix.aql.util;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.aql.expression.FunctionDecl;
import edu.uci.ics.asterix.aql.expression.Query;
import edu.uci.ics.asterix.aql.expression.VarIdentifier;
import edu.uci.ics.asterix.aql.parser.AQLParser;
import edu.uci.ics.asterix.aql.parser.ParseException;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.entities.Function;

public class FunctionUtil {


    public static FunctionDecl getFunctionDecl(Function function) throws AsterixException {
        String functionBody = function.getFunctionBody();
        List<String> params = function.getParams();
        List<VarIdentifier> varIdentifiers = new ArrayList<VarIdentifier>();

        StringBuilder builder = new StringBuilder();
        builder.append(" declare function " + function.getFunctionName());
        builder.append("(");
        for (String param : params) {
            VarIdentifier varId = new VarIdentifier(param);
            varIdentifiers.add(varId);
            builder.append(param);
            builder.append(",");
        }
        builder.delete(builder.length() - 1, builder.length());
        builder.append(")");
        builder.append("{");
        builder.append(functionBody);
        builder.append("}");
        AQLParser parser = new AQLParser(new StringReader(new String(builder)));

        Query query = null;
        try {
            query = (Query) parser.Statement();
        } catch (ParseException pe) {
            throw new AsterixException(pe);
        }

        FunctionDecl decl = (FunctionDecl) query.getPrologDeclList().get(0);
        return decl;
    }
}
