package edu.uci.ics.asterix.aql.context;

import edu.uci.ics.asterix.aql.parser.ScopeChecker;

public class RootScopeFactory {

    public static Scope createRootScope(ScopeChecker sc) {
        Scope rootScope = new Scope(sc);
        return rootScope;
    }

}
