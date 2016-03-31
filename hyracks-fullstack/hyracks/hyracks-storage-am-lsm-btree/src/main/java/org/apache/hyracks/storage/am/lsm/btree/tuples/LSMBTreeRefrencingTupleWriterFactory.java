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
package org.apache.hyracks.storage.am.lsm.btree.tuples;

import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.ophelpers.IndexOperation;
import org.apache.hyracks.storage.am.common.tuples.TypeAwareTupleWriterFactory;

/**
 * This is a TupleWriterFactory that can be used to reference the last writer it created
 * and switch between actual and antimatter writer modes
 * @author alamouda
 *
 */
public class LSMBTreeRefrencingTupleWriterFactory extends TypeAwareTupleWriterFactory{
	private static final long serialVersionUID = 1L;
	private final ITypeTraits[] typeTraits;
	private final int numKeyFields;
	private boolean isDelete;
	private LSMBTreeTupleWriter createdTupleWriter;

	public LSMBTreeRefrencingTupleWriterFactory(ITypeTraits[] typeTraits, int numKeyFields, boolean isDelete) {
		super(typeTraits);
		this.typeTraits = typeTraits;
		this.numKeyFields = numKeyFields;
		this.isDelete = isDelete;
	}

	@Override
	public ITreeIndexTupleWriter createTupleWriter() {
		createdTupleWriter = new LSMBTreeTupleWriter(typeTraits, numKeyFields, isDelete);
		return createdTupleWriter;
	}

	public void setMode(IndexOperation op){
		if(op == IndexOperation.INSERT){
			this.isDelete = false;
			if(createdTupleWriter != null){
				this.createdTupleWriter.setAntimatter(false);
			}
		} else if(op == IndexOperation.DELETE){
			this.isDelete = true;
			if(createdTupleWriter != null){
				this.createdTupleWriter.setAntimatter(true);
			}
		}
	}

	public LSMBTreeTupleWriter getCreatedTupleWriter() {
		return createdTupleWriter;
	}
}
