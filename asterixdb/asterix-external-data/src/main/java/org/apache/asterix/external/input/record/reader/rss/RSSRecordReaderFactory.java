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
package org.apache.asterix.external.input.record.reader.rss;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.asterix.common.dataflow.ICcApplicationContext;
import org.apache.asterix.external.api.IExternalDataSourceFactory;
import org.apache.asterix.external.api.IRecordReader;
import org.apache.asterix.external.api.IRecordReaderFactory;
import org.apache.asterix.external.util.ExternalDataConstants;
import org.apache.hyracks.algebricks.common.constraints.AlgebricksAbsolutePartitionConstraint;
import org.apache.hyracks.algebricks.common.exceptions.AlgebricksException;
import org.apache.hyracks.api.application.IServiceContext;
import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

import com.rometools.rome.feed.synd.SyndEntry;

public class RSSRecordReaderFactory implements IRecordReaderFactory<SyndEntry> {

    private static final long serialVersionUID = 1L;
    private final List<String> urls = new ArrayList<>();
    private transient AlgebricksAbsolutePartitionConstraint clusterLocations;
    private transient IServiceContext serviceContext;
    private static final List<String> recordReaderNames = Collections.unmodifiableList(Arrays.asList("rss_feed"));

    @Override
    public DataSourceType getDataSourceType() {
        return DataSourceType.RECORDS;
    }

    @Override
    public AlgebricksAbsolutePartitionConstraint getPartitionConstraint() throws AlgebricksException {
        int count = urls.size();
        clusterLocations = IExternalDataSourceFactory.getPartitionConstraints(
                (ICcApplicationContext) serviceContext.getApplicationContext(), clusterLocations, count);
        return clusterLocations;
    }

    @Override
    public void configure(IServiceContext serviceContext, Map<String, String> configuration) {
        this.serviceContext = serviceContext;
        String url = configuration.get(ExternalDataConstants.KEY_RSS_URL);
        if (url == null) {
            throw new IllegalArgumentException("no RSS URL provided");
        }
        initializeURLs(url);
    }

    private void initializeURLs(String url) {
        urls.clear();
        String[] rssURLs = url.split(",");
        for (String rssURL : rssURLs) {
            urls.add(rssURL);
        }
    }

    @Override
    public List<String> getRecordReaderNames() {
        return recordReaderNames;
    }

    @Override
    public boolean isIndexible() {
        return false;
    }

    @Override
    public IRecordReader<? extends SyndEntry> createRecordReader(IHyracksTaskContext ctx, int partition)
            throws HyracksDataException {
        try {
            return new RSSRecordReader(urls.get(partition));
        } catch (MalformedURLException e) {
            throw HyracksDataException.create(e);
        }
    }

    @Override
    public Class<? extends SyndEntry> getRecordClass() {
        return SyndEntry.class;
    }

}
