package org.jax.mvar.utility.parser;

import java.io.File;
import java.util.List;
import java.util.Map;

class SvAnnotationParser extends InfoParser {

    /**
     * Constructor
     *
     * @param file
     */
    public SvAnnotationParser(File file) {
        super(file);
    }

    @Override
    public String getInfoId() {
        return "SVANN";
    }

    /**
     * Parse string in the following form:
     *  ##INFO=<ID=SVANN,Number=1,Type=String,Description="Functional SV Annotation:'Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|ERRORS / WARNINGS / INFO'">
     * @param infoString
     * @return list of maps
     */
    @Override
    public List<Map<String, String>> parse(String infoString) throws Exception {
        // ##INFO=<ID=SVANN,Number=1,Type=String,Description="Functional SV Annotation:'Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|ERRORS / WARNINGS / INFO'">
        return super.parse(infoString);
    }
}
