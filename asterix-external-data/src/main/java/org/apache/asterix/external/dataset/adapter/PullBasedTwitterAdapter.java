/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.external.dataset.adapter;

import java.util.Map;

import org.apache.asterix.common.exceptions.AsterixException;
import org.apache.asterix.common.feeds.api.IFeedAdapter;
import org.apache.asterix.common.parse.ITupleForwardPolicy;
import org.apache.asterix.om.types.ARecordType;
import org.apache.asterix.runtime.operators.file.AsterixTupleParserFactory;
import org.apache.asterix.runtime.operators.file.CounterTimerTupleForwardPolicy;
import org.apache.hyracks.api.context.IHyracksTaskContext;

/**
 * An adapter that provides the functionality of receiving tweets from the
 * Twitter service in the form of ADM formatted records.
 */
public class PullBasedTwitterAdapter extends ClientBasedFeedAdapter implements IFeedAdapter {

    private static final long serialVersionUID = 1L;

    private static final int DEFAULT_BATCH_SIZE = 5;

    private ARecordType recordType;
    private PullBasedTwitterFeedClient tweetClient;

    @Override
    public IFeedClient getFeedClient(int partition) {
        return tweetClient;
    }

    public PullBasedTwitterAdapter(Map<String, String> configuration, ARecordType recordType, IHyracksTaskContext ctx)
            throws AsterixException {
        super(configuration, ctx);
        tweetClient = new PullBasedTwitterFeedClient(ctx, recordType, this);
    }

    public ARecordType getAdapterOutputType() {
        return recordType;
    }

    @Override
    public DataExchangeMode getDataExchangeMode() {
        return DataExchangeMode.PULL;
    }

    @Override
    public boolean handleException(Exception e) {
        return true;
    }

    @Override
    public ITupleForwardPolicy getTupleParserPolicy() {
        configuration.put(ITupleForwardPolicy.PARSER_POLICY,
                ITupleForwardPolicy.TupleForwardPolicyType.COUNTER_TIMER_EXPIRED.name());
        String propValue = configuration.get(CounterTimerTupleForwardPolicy.BATCH_SIZE);
        if (propValue == null) {
            configuration.put(CounterTimerTupleForwardPolicy.BATCH_SIZE, "" + DEFAULT_BATCH_SIZE);
        }
        return AsterixTupleParserFactory.getTupleParserPolicy(configuration);
    }

}
