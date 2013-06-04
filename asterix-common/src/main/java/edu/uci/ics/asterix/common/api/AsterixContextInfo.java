package edu.uci.ics.asterix.common.api;

import edu.uci.ics.hyracks.api.application.ICCApplicationContext;

public class AsterixContextInfo implements IAsterixContextInfo {

    public static AsterixContextInfo INSTANCE;

    private final ICCApplicationContext appCtx;

    public static void initialize(ICCApplicationContext ccAppCtx) {
        if (INSTANCE == null) {
            INSTANCE = new AsterixContextInfo(ccAppCtx);
        }
    }

    private AsterixContextInfo(ICCApplicationContext ccAppCtx) {
        this.appCtx = ccAppCtx;
    }

    @Override
    public ICCApplicationContext getCCApplicationContext() {
        return appCtx;
    }

}
