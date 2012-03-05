/*
 * Copyright 2009-2011 by The Regents of the University of California
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
package edu.uci.ics.asterix.feed.managed.adapter;

import edu.uci.ics.asterix.external.data.adapter.api.IDatasourceReadAdapter;

public interface IManagedFeedAdapter extends IDatasourceReadAdapter {

	public enum OperationState {
		SUSPENDED,
		// INACTIVE state signifies that the feed dataset is not
		// connected with the external world through the feed
		// adapter.
		ACTIVE,
		// ACTIVE state signifies that the feed dataset is connected to the
		// external world using an adapter that may put data into the dataset.
		STOPPED, INACTIVE
	}
	
	public void beforeSuspend() throws Exception;

	public void beforeResume() throws Exception;

	public void beforeStop() throws Exception;

	public void stop() throws Exception;

}
