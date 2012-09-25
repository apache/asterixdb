/*
 * Copyright 2009-2012 by The Regents of the University of California
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
package edu.uci.ics.hyracks.storage.common.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public abstract class AbstractIndexLocalResourceClass implements ILocalResourceClass {

    @Override
    public byte[] serialize(ILocalResource resource) throws HyracksDataException {
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oosToBaos = null;

        try {
            baos = new ByteArrayOutputStream();
            oosToBaos = new ObjectOutputStream(baos);
            oosToBaos.writeLong(resource.getResourceId());
            oosToBaos.writeUTF(resource.getResourceName());
            oosToBaos.writeObject(resource.getResourceObject());
            return baos.toByteArray();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        } finally {
            if (oosToBaos != null) {
                try {
                    oosToBaos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

            if (oosToBaos == null && baos != null) {
                try {
                    baos.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    @Override
    public ILocalResource deserialize(byte[] bytes) throws HyracksDataException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream oisFromBais = null;
        try {
            oisFromBais = new ObjectInputStream(bais);
            return new IndexLocalResource(oisFromBais.readLong(), oisFromBais.readUTF(), oisFromBais.readObject(), this);
        } catch (Exception e) {
            throw new HyracksDataException(e);
        } finally {
            if (oisFromBais != null) {
                try {
                    oisFromBais.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
            if (oisFromBais == null && bais != null) {
                try {
                    bais.close();
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }
    }

    public abstract int getResourceClassId();
}
