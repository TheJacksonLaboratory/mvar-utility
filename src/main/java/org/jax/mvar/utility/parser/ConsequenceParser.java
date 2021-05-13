package org.jax.mvar.utility.parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * ##INFO=<ID=CSQ,Number=.,Type=String,Description="Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Gene|Feature|Feature_type|Consequence|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND">
 */
public class ConsequenceParser extends InfoParser {

    private static final List<String> ANNOTATION_KEYS = Arrays.asList("Allele","Consequence","IMPACT","SYMBOL","Gene","Feature_type","Feature","BIOTYPE",
            "EXON","INTRON","HGVSc","HGVSp","cDNA_position","CDS_position","Protein_position","Amino_acids",
            "Codons","Existing_variation","DISTANCE","STRAND","FLAGS","SYMBOL_SOURCE","HGNC_ID","HGVSg","CLIN_SIG","SOMATIC","PHENO");
    @Override
    public String getInfoId() {
        return "CSQ";
    }

    @Override
    public List<Map<String, String>> parse(String infoString) throws Exception {
        // Consequence type from Ensembl 78 as predicted by VEP. Format: Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|DISTANCE|STRAND|FLAGS|SYMBOL_SOURCE|HGNC_ID|HGVSg|CLIN_SIG|SOMATIC|PHENO
        return super.parse(infoString);
    }

    @Override
    public List<String> getAnnotationKeys() {
        return ANNOTATION_KEYS;
    }

    /**
     * Returns the rs Id parsed from the given consequence string
     * @param consequence Vep annotation string
     * @param id CSQ id (Allele for instance)
     * @return
     */
    public static String getCSQ(String consequence, String  id) throws Exception {
        int idx = ANNOTATION_KEYS.indexOf(id);
        if (consequence != null && !consequence.isEmpty()) {
            return consequence.split("\\|")[idx];
        }
        return "";
    }

    /**
     * Returns the rs Id parsed from the given consequence string
     * @param consequence Vep annotation string
     * @return a list of two values : 1rst is rsId and 2nd is hgvs
     */
    public static List<String> getRsIDAndHGVS(String consequence) throws Exception {
        int rsIdIdx = ANNOTATION_KEYS.indexOf("Existing_variation");
        int hgvsIdx = ANNOTATION_KEYS.indexOf("HGVSg");
        if (consequence != null && !consequence.isEmpty()) {
            List<String> result = new ArrayList<>();
            String[] csqs = consequence.split("\\|");
            result.add(csqs[rsIdIdx]);
            String hgvsg;
            if (csqs[hgvsIdx].contains(":")) {
                // we remove the suffix with "chr:" if any
                hgvsg = csqs[hgvsIdx].split(":")[1];
            } else {
                hgvsg = csqs[hgvsIdx];
            }
            result.add(hgvsg);
            return result;
        }
        return null;
    }
}
