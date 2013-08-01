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
package edu.uci.ics.hivesterix.runtime.evaluator;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.BytesWritable;

import edu.uci.ics.hivesterix.serde.lazy.LazyFactory;
import edu.uci.ics.hivesterix.serde.lazy.LazyObject;
import edu.uci.ics.hivesterix.serde.lazy.LazySerDe;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.runtime.base.ICopyEvaluator;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

@SuppressWarnings("deprecation")
public abstract class AbstractExpressionEvaluator implements ICopyEvaluator {

    private List<ICopyEvaluator> children;

    private ExprNodeEvaluator evaluator;

    private IDataOutputProvider out;

    private ObjectInspector inspector;

    /**
     * output object inspector
     */
    private ObjectInspector outputInspector;

    /**
     * cached row object
     */
    private LazyObject<? extends ObjectInspector> cachedRowObject;

    /**
     * serializer/derialzer for lazy object
     */
    private SerDe lazySer;

    /**
     * data output
     */
    DataOutput dataOutput;

    public AbstractExpressionEvaluator(ExprNodeEvaluator hiveEvaluator, ObjectInspector oi, IDataOutputProvider output)
            throws AlgebricksException {
        evaluator = hiveEvaluator;
        out = output;
        inspector = oi;
        dataOutput = out.getDataOutput();
    }

    protected ObjectInspector getRowInspector() {
        return null;
    }

    protected IDataOutputProvider getIDataOutputProvider() {
        return out;
    }

    protected ExprNodeEvaluator getHiveEvaluator() {
        return evaluator;
    }

    public ObjectInspector getObjectInspector() {
        return inspector;
    }

    @Override
    public void evaluate(IFrameTupleReference r) throws AlgebricksException {
        // initialize hive evaluator
        try {
            if (outputInspector == null)
                outputInspector = evaluator.initialize(inspector);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AlgebricksException(e.getMessage());
        }

        readIntoCache(r);
        try {
            Object result = evaluator.evaluate(cachedRowObject);

            // if (result == null) {
            // result = evaluator.evaluate(cachedRowObject);
            //
            // // check if result is null
            //
            // String errorMsg = "serialize null object in  \n output " +
            // outputInspector.toString() + " \n input "
            // + inspector.toString() + "\n ";
            // errorMsg += "";
            // List<Object> columns = ((StructObjectInspector)
            // inspector).getStructFieldsDataAsList(cachedRowObject);
            // for (Object column : columns) {
            // errorMsg += column.toString() + " ";
            // }
            // errorMsg += "\n";
            // Log.info(errorMsg);
            // System.out.println(errorMsg);
            // // result = new BooleanWritable(true);
            // throw new IllegalStateException(errorMsg);
            // }

            serializeResult(result);
        } catch (HiveException e) {
            e.printStackTrace();
            throw new AlgebricksException(e.getMessage());
        } catch (IOException e) {
            e.printStackTrace();
            throw new AlgebricksException(e.getMessage());
        }
    }

    /**
     * serialize the result
     * 
     * @param result
     *            the evaluation result
     * @throws IOException
     * @throws AlgebricksException
     */
    private void serializeResult(Object result) throws IOException, AlgebricksException {
        if (lazySer == null)
            lazySer = new LazySerDe();

        try {
            BytesWritable outputWritable = (BytesWritable) lazySer.serialize(result, outputInspector);
            dataOutput.write(outputWritable.getBytes(), 0, outputWritable.getLength());
        } catch (SerDeException e) {
            throw new AlgebricksException(e);
        }
    }

    /**
     * bind the tuple reference to the cached row object
     * 
     * @param r
     */
    private void readIntoCache(IFrameTupleReference r) {
        if (cachedRowObject == null)
            cachedRowObject = (LazyObject<? extends ObjectInspector>) LazyFactory.createLazyObject(inspector);
        cachedRowObject.init(r);
    }

    /**
     * set a list of children of this evaluator
     * 
     * @param children
     */
    public void setChildren(List<ICopyEvaluator> children) {
        this.children = children;
    }

    public void addChild(ICopyEvaluator child) {
        if (children == null)
            children = new ArrayList<ICopyEvaluator>();
        children.add(child);
    }

}
