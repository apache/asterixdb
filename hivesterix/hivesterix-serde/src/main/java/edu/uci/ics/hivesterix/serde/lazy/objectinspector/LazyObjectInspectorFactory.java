package edu.uci.ics.hivesterix.serde.lazy.objectinspector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * ObjectInspectorFactory is the primary way to create new ObjectInspector
 * instances.
 * SerDe classes should call the static functions in this library to create an
 * ObjectInspector to return to the caller of SerDe2.getObjectInspector().
 * The reason of having caches here is that ObjectInspectors do not have an
 * internal state - so ObjectInspectors with the same construction parameters
 * should result in exactly the same ObjectInspector.
 */

public final class LazyObjectInspectorFactory {

    static ConcurrentHashMap<ArrayList<Object>, LazyColumnarObjectInspector> cachedLazyColumnarObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyColumnarObjectInspector>();

    static ConcurrentHashMap<ArrayList<Object>, LazyStructObjectInspector> cachedLazyStructObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyStructObjectInspector>();

    static ConcurrentHashMap<ArrayList<Object>, LazyListObjectInspector> cachedLazyListObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyListObjectInspector>();

    static ConcurrentHashMap<ArrayList<Object>, LazyMapObjectInspector> cachedLazyMapObjectInspector = new ConcurrentHashMap<ArrayList<Object>, LazyMapObjectInspector>();

    public static LazyColumnarObjectInspector getLazyColumnarObjectInspector(List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(structFieldNames);
        signature.add(structFieldObjectInspectors);
        LazyColumnarObjectInspector result = cachedLazyColumnarObjectInspector.get(signature);
        if (result == null) {
            result = new LazyColumnarObjectInspector(structFieldNames, structFieldObjectInspectors);
            cachedLazyColumnarObjectInspector.put(signature, result);
        }
        return result;
    }

    public static LazyStructObjectInspector getLazyStructObjectInspector(List<String> structFieldNames,
            List<ObjectInspector> structFieldObjectInspectors) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(structFieldNames);
        signature.add(structFieldObjectInspectors);
        LazyStructObjectInspector result = cachedLazyStructObjectInspector.get(signature);
        if (result == null) {
            result = new LazyStructObjectInspector(structFieldNames, structFieldObjectInspectors);
            cachedLazyStructObjectInspector.put(signature, result);
        }
        return result;
    }

    public static LazyListObjectInspector getLazyListObjectInspector(ObjectInspector listElementInspector) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(listElementInspector);
        LazyListObjectInspector result = cachedLazyListObjectInspector.get(signature);
        if (result == null) {
            result = new LazyListObjectInspector(listElementInspector);
            cachedLazyListObjectInspector.put(signature, result);
        }
        return result;
    }

    public static LazyMapObjectInspector getLazyMapObjectInspector(ObjectInspector keyInspector,
            ObjectInspector valueInspector) {
        ArrayList<Object> signature = new ArrayList<Object>();
        signature.add(keyInspector);
        signature.add(valueInspector);
        LazyMapObjectInspector result = cachedLazyMapObjectInspector.get(signature);
        if (result == null) {
            result = new LazyMapObjectInspector(keyInspector, valueInspector);
            cachedLazyMapObjectInspector.put(signature, result);
        }
        return result;
    }

    private LazyObjectInspectorFactory() {
        // prevent instantiation
    }
}
