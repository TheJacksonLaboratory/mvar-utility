package org.jax.mvar.utility.parser;


import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;

import java.util.List;
import java.util.Map;

/**
 * This class is the base class to parse the INFO column from a variant entry.
 *
 * ##INFO=<ID=ANN,Number=1,Type=String,Description="Functional annotations:'Allele|Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|Rank|HGVS.c|HGVS.p|cDNA.pos / cDNA.length|CDS.pos / CDS.length|AA.pos / AA.length|Distance|ERRORS / WARNINGS / INFO'">
 * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND">
 * ##INFO=<ID=DP,Number=1,Type=Integer,Description="Raw read depth">
 * ##INFO=<ID=DP4,Number=4,Type=Integer,Description="Total Number of high-quality ref-fwd, ref-reverse, alt-fwd and alt-reverse bases">
 * ##INFO=<ID=INDEL,Number=0,Type=Flag,Description="Indicates that the variant is an INDEL.">
 * ##INFO=<ID=SVANN,Number=1,Type=String,Description="Functional SV Annotation:'Annotation|Annotation_Impact|Gene_Name|Gene_ID|Feature_Type|Feature_ID|Transcript_BioType|ERRORS / WARNINGS / INFO'">
 */
abstract public class InfoParser {

    String[] infos;

    /**
     * Returns the ID for the INFO column. To be implemented in child class.
     * @return
     */
    abstract public String getInfoId();

    /**
     * Returns the expected length of this functional annotation
     * @return
     */
    int getInfoLength() {
        return getAnnotationKeys().size();
    }

    /**
     * Returns the expected object for the given ID implementation parser.
     * Can be overriden if necessary
     * @param infoString string to be parsed
     * @return list of maps
     */
    public List<Map<String, String>> parse(String infoString) throws Exception {
        if (!infoString.contains(getInfoId())) {
            throw new IllegalArgumentException("This INFO string does not have the " + getInfoId() + " id.");
        }
        // split by " 'id'= "
        StringBuilder strSeparator = new StringBuilder(getInfoId());
        strSeparator.append('=');
        infos = infoString.split(strSeparator.toString());
        if (infos.length > 1) {
            // split string by commas: a comma in the jannovar string separates multiple transcripts
            String[] functAnnotations = infos[1].split(";")[0].split(",");
            List<Map<String, String>> listOfAnnMap = new FastList<>(functAnnotations.length);
            for (int i = 0; i < functAnnotations.length; i++) {
                String[] infoAnnArray = functAnnotations[i].split("\\|", -1);
                if (infoAnnArray.length != getInfoLength()) {
                    throw new IllegalArgumentException("Expecting " + getInfoId() + " identifier to have " + getInfoLength() + " blocks. Had " + infoAnnArray.length + " instead.");
                }
                Map<String, String> annMap = new UnifiedMap();
                int index = 0;
                for (String annotationKey:getAnnotationKeys()) {
                    annMap.put(annotationKey, infoAnnArray[index]);
                    index++;
                }
                listOfAnnMap.add(annMap);
            }
            return listOfAnnMap;
        }
        return null;
    }

    /**
     * List of annotation keys for the given implementation
     * @return
     */
    abstract public List<String> getAnnotationKeys();

    /**
     *
     * @param annotations
     * @param id
     * @return
     */
    public static String getAnnotation(String[] annotations, String id) {
        for (int i = 0; i < annotations.length; i++) {
            if (annotations[i].startsWith(id)) {
                return annotations[i];
            }
        }
        return "";
    }
}
