package org.jax.mvar.parser;

import java.util.HashMap;
import java.util.Map;

class FullInfoParser {

    Map<String, Object> infosDataMap;

    /**
     * ##INFO=<ID=ANN,Number=1,Type=String,Description="Functional annotations:'Allele|Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|Rank|HGVS.c|HGVS.p|cDNA.pos / cDNA.length|CDS.pos / CDS.length|AA.pos / AA.length|Distance|ERRORS / WARNINGS / INFO'">
     * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND">
     * ##INFO=<ID=DP,Number=1,Type=Integer,Description="Raw read depth">
     * ##INFO=<ID=DP4,Number=4,Type=Integer,Description="Total Number of high-quality ref-fwd, ref-reverse, alt-fwd and alt-reverse bases">
     * ##INFO=<ID=INDEL,Number=0,Type=Flag,Description="Indicates that the variant is an INDEL.">
     * ##INFO=<ID=SVANN,Number=1,Type=String,Description="Functional SV Annotation:'Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|ERRORS / WARNINGS / INFO'">
     * @param infoString
     */
    FullInfoParser(String infoString) throws Exception {
        infosDataMap = new HashMap<>();
        AnnotationParser annParser = new AnnotationParser();
        infosDataMap.put("ANN", annParser.parse(infoString));
        ConsequenceParser csqParser = new ConsequenceParser();
        infosDataMap.put("CSQ", csqParser.parse(infoString));
        DP4Parser dp4Parser = new DP4Parser();
        infosDataMap.put("DP4", dp4Parser.parse(infoString));
        DPParser dpParser = new DPParser();
        infosDataMap.put("DP", dpParser.parse(infoString));
        SvAnnotationParser svAnnotParser = new SvAnnotationParser();
        infosDataMap.put("SVANN", svAnnotParser.parse(infoString));
    }

}
