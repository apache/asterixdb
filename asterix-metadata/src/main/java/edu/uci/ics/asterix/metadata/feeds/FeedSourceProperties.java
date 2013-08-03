package edu.uci.ics.asterix.metadata.feeds;

import java.util.HashMap;
import java.util.Map;

public class FeedSourceProperties {

    public enum ExchangeMode {
        PULL,
        PUSH
    }

    public enum IngestionRuntimeType {
        STATELESS,
        STATEFULL
    }

    public enum ComputeRuntimeType {
        STATELESS,
        STATEFULL
    }

    public static class FeedSourcePropertyKeys {
        public static final String EXCHANGE_MODE = "exchange_mode";
        public static final String INGESTION_RUNTIME_TYPE = "ingestion_runtime_type";
        public static final String COMPUTE_RUNTIME_TYPE = "compute_runtime_type";
        public static final String BACKWARD_TIME_TRAVEL = "backward_time_travel";
        public static final String COMPUTE_IDEMPOTENCE = "compute_idempotence";
    }

    private Map<String, String> sourceConfiguration;

    public FeedSourceProperties(Map<String, String> sourceConfiguration) {
        this.sourceConfiguration = sourceConfiguration;
    }

    public static class FeedSourcePropertyAccessor {

        private boolean computeIdempotence;
        private boolean backwardTimeTravel;
        private ExchangeMode exchangeMode;
        private IngestionRuntimeType ingestionRuntimeType;
        private ComputeRuntimeType computeRuntimeType;

    }
}
