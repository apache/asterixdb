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
package org.apache.asterix.test.server;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.asterix.common.config.GlobalConfig;
import org.apache.hyracks.http.api.IServletRequest;
import org.apache.hyracks.http.api.IServletResponse;
import org.apache.hyracks.http.server.AbstractServlet;
import org.apache.hyracks.http.server.utils.HttpUtil;
import org.apache.logging.log4j.Level;

import com.rometools.rome.feed.synd.SyndContent;
import com.rometools.rome.feed.synd.SyndContentImpl;
import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndEntryImpl;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.feed.synd.SyndFeedImpl;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedOutput;

import io.netty.handler.codec.http.HttpResponseStatus;

public class RSSFeedServlet extends AbstractServlet {

    private static final String DEFAULT_FEED_TYPE = "default.feed.type";
    private static final String FEED_TYPE = "type";
    private static final String MIME_TYPE = "application/xml; charset=UTF-8";
    private static final String COULD_NOT_GENERATE_FEED_ERROR = "Could not generate feed";

    private static final DateFormat DATE_PARSER = new SimpleDateFormat("yyyy-MM-dd");
    private String defaultFeedType;

    public RSSFeedServlet(ConcurrentMap<String, Object> ctx, String[] paths) {
        super(ctx, paths);
        defaultFeedType = (String) ctx.get(DEFAULT_FEED_TYPE);
        defaultFeedType = (defaultFeedType != null) ? defaultFeedType : "atom_0.3";
    }

    @Override
    protected void get(IServletRequest req, IServletResponse res) throws IOException {
        try {
            SyndFeed feed = getFeed(req);
            String feedType = req.getParameter(FEED_TYPE);
            feedType = (feedType != null) ? feedType : defaultFeedType;
            feed.setFeedType(feedType);
            HttpUtil.setContentType(res, MIME_TYPE, req);
            SyndFeedOutput output = new SyndFeedOutput();
            output.output(feed, res.writer());
        } catch (FeedException | ParseException ex) {
            GlobalConfig.ASTERIX_LOGGER.log(Level.WARN, ex.getMessage(), ex);
            res.writer().print(COULD_NOT_GENERATE_FEED_ERROR);
            res.setStatus(HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

    protected SyndFeed getFeed(IServletRequest req) throws IOException, FeedException, ParseException {
        SyndFeed feed = new SyndFeedImpl();
        feed.setTitle("Sample Feed (created with ROME)");
        feed.setLink("http://rome.dev.java.net");
        feed.setDescription("This feed has been created using ROME (Java syndication utilities");

        List<SyndEntry> entries = new ArrayList<>();
        SyndEntry entry;
        SyndContent description;

        entry = new SyndEntryImpl();
        entry.setTitle("AsterixDB 0.8.7");
        entry.setLink("http://http://asterixdb.apache.org/docs/0.8.7-incubating/index.html");
        entry.setPublishedDate(DATE_PARSER.parse("2012-06-08"));
        description = new SyndContentImpl();
        description.setType("text/plain");
        description.setValue("AsterixDB 0.8.7 Release");
        entry.setDescription(description);
        entries.add(entry);

        entry = new SyndEntryImpl();
        entry.setTitle("Couchbase 4.1");
        entry.setLink("http://blog.couchbase.com/2015/december/introducing-couchbase-server-4.1");
        entry.setPublishedDate(DATE_PARSER.parse("2015-12-09"));
        description = new SyndContentImpl();
        description.setType("text/plain");
        description.setValue("Couchbase Server 4.1 Release. Bug fixes, minor API changes and some new features");
        entry.setDescription(description);
        entries.add(entry);

        entry = new SyndEntryImpl();
        entry.setTitle("ROME v0.3");
        entry.setLink("http://wiki.java.net/bin/view/Javawsxml/rome03");
        entry.setPublishedDate(DATE_PARSER.parse("2004-07-27"));
        description = new SyndContentImpl();
        description.setType("text/html");
        description.setValue("<p>Bug fixes, API changes, some new features and some Unit testing</p>"
                + "<p>For details check the <a href=\"https://rometools.jira.com/wiki/display/ROME/Change+Log#"
                + "ChangeLog-Changesmadefromv0.3tov0.4\">Changes Log for 0.3</a></p>");
        entry.setDescription(description);
        entries.add(entry);

        entry = new SyndEntryImpl();
        entry.setTitle("ROME v0.4");
        entry.setLink("http://wiki.java.net/bin/view/Javawsxml/rome04");
        entry.setPublishedDate(DATE_PARSER.parse("2004-09-24"));
        description = new SyndContentImpl();
        description.setType("text/html");
        description.setValue("<p>Bug fixes, API changes, some new features, Unit testing completed</p>"
                + "<p>For details check the <a href=\"https://rometools.jira.com/wiki/display/ROME/Change+Log#"
                + "ChangeLog-Changesmadefromv0.4tov0.5\">Changes Log for 0.4</a></p>");
        entry.setDescription(description);
        entries.add(entry);
        feed.setEntries(entries);
        return feed;
    }
}
