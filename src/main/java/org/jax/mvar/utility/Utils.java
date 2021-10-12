package org.jax.mvar.utility;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class Utils {

    /**
     * Makes a copy of a List
     * @param list
     * @param <T>
     * @return
     */
    public static <T> List<?> copyList(List<T> list) {
        List<T> copiedList = new ArrayList<>();
        for (T item : list) {
            copiedList.add(item);
        }
        return copiedList;
    }

    /**
     * Makes a copy of a Map
     * @param map
     * @param <K>
     * @param <V>
     * @return
     */
    public static <K, V> Map<K, V> copyMap(Map<?, ?> map) {
        Map<K, V> copiedMap = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            K obj = (K) entry.getKey();
            V val = (V) entry.getValue();
            copiedMap.put(obj, val);
        }
        return copiedMap;
    }
}
