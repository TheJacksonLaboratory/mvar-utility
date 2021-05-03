package org.jax.mvar.utility.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND">
 */
public class ConsequenceParser extends InfoParser {

    @Override
    public String getInfoId() {
        return "CSQ";
    }

    @Override
    public List<Map<String, String>> parse(String infoString) throws Exception {
        // Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND
        return super.parse(infoString);
    }

    @Override
    public List<String> getAnnotationKeys() {
        return Arrays.asList("Allele", "Gene", "Feature", "Feature_type", "Consequence", "cDNA_position",
                "CDS_position", "Protein_position", "Amino_acids", "Codons", "Existing_variation",
                "DISTANCE", "STRAND");
    }

    /**
     * Returns the HGVSg parsed from the given consequence string
     * @param consequence Vep annotation string
     * @param idx index where the corresponding hgvsg is
     * @return
     */
    public static String getHGVSg(String consequence, int idx) throws Exception {
        if (consequence != null && !consequence.equals("")) {
            String[] csqParts = consequence.split(",");
            if (idx >= csqParts.length)
                throw new Exception("idx must be within range 0 to " + csqParts.length);
            return csqParts[idx].substring(csqParts[idx].lastIndexOf('|') + 1);
        }
        return "";
    }
}
