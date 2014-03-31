package edu.uci.ics.asterix.api.http.servlet;

import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import edu.uci.ics.asterix.aql.base.Statement.Kind;

public class AQLAPIServlet extends RESTAPIServlet {

    private static final long serialVersionUID = 1L;

    private static final String AQL_STMT_PARAM_NAME = "aql";

    private static final List<Kind> allowedStatements = new ArrayList<>();

    static {
        for (Kind k : Kind.values()) {
            allowedStatements.add(k);
        }
    }

    @Override
    protected String getQueryParameter(HttpServletRequest request) {
        return request.getParameter(AQL_STMT_PARAM_NAME);
    }

    @Override
    protected List<Kind> getAllowedStatements() {
        return allowedStatements;
    }

    @Override
    protected String getErrorMessage() {
        throw new IllegalStateException();
    }

}
