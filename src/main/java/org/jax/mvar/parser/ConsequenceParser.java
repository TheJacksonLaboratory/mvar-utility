package org.jax.mvar.parser;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND">
 */
class ConsequenceParser extends InfoParser {

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
}
