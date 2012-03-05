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
package edu.uci.ics.asterix.external.data.adapter.api;

import edu.uci.ics.asterix.external.data.parser.IDataParser;

public interface IDatasourceReadAdapter extends IDatasourceAdapter {

	/**
	 * Retrieves data from an external datasource, packs it in frames and uses a
	 * frame writer to flush the frames to a recipient operator.
	 * 
	 * @param partition
	 *            Multiple instances of the adapter can be configured to
	 *            retrieve data in parallel. Partition is an integer between 0
	 *            to N-1 where N is the number of parallel adapter instances.
	 *            The partition value helps configure a particular instance of
	 *            the adapter to fetch data.
	 * @param writer
	 *            An instance of IFrameWriter that is used to flush frames to
	 *            the recipient operator
	 * @throws Exception
	 */
	
	public IDataParser getDataParser(int partition) throws Exception;
	

}
