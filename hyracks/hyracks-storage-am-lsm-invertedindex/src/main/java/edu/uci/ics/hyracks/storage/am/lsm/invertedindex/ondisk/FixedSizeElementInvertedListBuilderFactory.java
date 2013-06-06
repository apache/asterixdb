/*
 * Copyright 2009-2013 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.ondisk;

import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilder;
import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.api.IInvertedListBuilderFactory;

public class FixedSizeElementInvertedListBuilderFactory implements IInvertedListBuilderFactory {

    private final ITypeTraits[] invListFields;

    public FixedSizeElementInvertedListBuilderFactory(ITypeTraits[] invListFields) {
        this.invListFields = invListFields;
    }

    @Override
    public IInvertedListBuilder create() {
        return new FixedSizeElementInvertedListBuilder(invListFields);
    }
}
