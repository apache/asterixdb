/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.feed.mgmt;


/**
 * Provider for all the sub-systems (transaction/lock/log/recovery) managers.
 * Users of transaction sub-systems must obtain them from the provider.
 */
public class FeedSystemProvider {
    private static final IFeedManager feedManager = new FeedManager();
    
    public static IFeedManager getFeedManager()  {
      return feedManager;
    }
}