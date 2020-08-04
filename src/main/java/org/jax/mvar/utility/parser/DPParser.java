package org.jax.mvar.utility.parser;

import java.util.*;

/**
 * ##INFO=<ID=DP,Number=1,Type=Integer,Description="Raw read depth">
 */
class DPParser extends InfoParser {

    @Override
    public String getInfoId() {
        return "DP";
    }

    @Override
    public List<Map<String, String>> parse(String infoString) {
        // split by " 'id'= "
        infos = infoString.split(getInfoId() + "=");
        Map<String, String> dpMap = new HashMap<>();
        if (infos.length > 1) {
            String val = infos[1].split(";")[0];
            dpMap.put(getAnnotationKeys().get(0), val);
        }
        List<Map<String, String>> result = new ArrayList<>();
        result.add(dpMap);
        return result;
    }

    @Override
    public List<String> getAnnotationKeys() {
        return Arrays.asList("Raw read depth");
    }
}
