package org.jax.mvar.parser;

import java.util.*;

/**
 * ##INFO=<ID=DP4,Number=4,Type=Integer,Description="Total Number of high-quality ref-fwd, ref-reverse, alt-fwd and alt-reverse bases">
 */
class DP4Parser extends InfoParser {

    @Override
    public String getInfoId() {
        return "DP4";
    }

    @Override
    public List<Map<String, String>> parse(String infoString) throws Exception {
        // split by " 'id'= "
        infos = infoString.split(getInfoId() + "=");
        Map<String, String> dp4Map = new HashMap<>();
        if (infos.length > 1) {
            // Total Number of high-quality ref-fwd, ref-reverse, alt-fwd and alt-reverse bases
            // remove all string after tab
            String cleanDP4 = infos[1].split("\t")[0];
            String[] dp4Info = cleanDP4.split(",");
            if (dp4Info.length != getInfoLength()) {
                throw new Exception("Expecting " + getInfoId() + " identifier to have " + getInfoLength() + " blocks. Had " + dp4Info.length + " instead.");
            }
            dp4Map.put("ref-fwd", dp4Info[0]);
            dp4Map.put("ref-reverse", dp4Info[1]);
            dp4Map.put("alt-fwd", dp4Info[2]);
            dp4Map.put("alt-reverse", dp4Info[3]);
        }
        List<Map<String, String>> result = new ArrayList<>();
        result.add(dp4Map);
        return result;
    }

    @Override
    public List<String> getAnnotationKeys() {
        return Arrays.asList("ref-fwd", "ref-reverse", "alt-fwd", "alt-reverse");
    }
}
