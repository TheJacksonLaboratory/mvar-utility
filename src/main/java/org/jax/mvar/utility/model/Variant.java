package org.jax.mvar.utility.model;

import java.util.HashMap;
import java.util.Map;

public class Variant {
    String chr;
    String pos;
    String id;
    String ref;
    String alt;
    String qual;
    String filter;
    String hgvsg;
    String jannovarAnnotation;
    String info;
    String format;
    String genotypeData;
    String vepConsequence;
    String type;
    String variantRefTxt;
    Map strains;
    String[] strainList;

    public Variant(String chr, String pos, String id, String ref, String alt, String qual, String filter,
                   String format, String hgvsg, String jannovarAnnotation, String genotypeData) {
        this.chr = chr.replace("ch", "").replace("r", "");
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.format = format;
        this.hgvsg = hgvsg;
        this.jannovarAnnotation = jannovarAnnotation;
        this.genotypeData = genotypeData;
        this.variantRefTxt = chr.concat("_").concat(pos).concat("_").concat(ref).concat("_").concat(alt);
        setType(this.ref, this.alt);
    }

    /**
     * Convert reference and alternate allele strings into a
     * mutation type, being one of "SNP","INS","DEL","GAIN"
     * or "LOSS", with the latter two representing CNVs.
     *
     * @param refSeq reference sequence
     * @param altSeq alternative sequence
     */
    private void setType(String refSeq, String altSeq) {
        String result = "SNP";
        if (altSeq.equals("DUP") || altSeq.equals("<DUP>"))
            result = "GAIN";
        else if (altSeq.equals("DEL") || altSeq.equals("<DEL>"))
            result = "LOSS";
        else if (altSeq.equals("INV") || altSeq.equals("<INV>"))
            result = "INV";
        else if (refSeq.length() < altSeq.length())
            result = "INS";
        else if (refSeq.length() > altSeq.length())
            result = "DEL";
        this.type = result;
    }

    public String getChr() {
        return chr;
    }

    public String getPos() {
        return pos;
    }

    public String getId() {
        return id;
    }

    public String getRef() {
        return ref;
    }

    public String getAlt() { return alt; }

    public String getHgvsg() {
        return hgvsg;
    }

    public String getQual() { return qual; }

    public String getFilter() { return filter; }

    public String getFormat() { return format; }

    public String getGenotypeData() { return genotypeData; }

    public String getAnnotation() {
        return jannovarAnnotation;
    }

    public String getVariantRefTxt() {
        return variantRefTxt;
    }

    public String getType() {
        return type;
    }

    private void setStrains(String genotypeData, String[] strainsList) throws Exception {
        String[] genotypes = genotypeData.split("\t");
        if (genotypes.length != strainsList.length)
            throw new Exception("Genotypes and StrainList must have the size.");
        strains = new HashMap();
        for (int i = 0; i < strainsList.length; i++) {
            if (!genotypes[i].startsWith("./.")) {
                strains.put(strainsList[i], genotypes[i]);
            }
        }
    }

    /**
     *
     * @return a Map of genotype data (value) for strains (key)
     */
    public Map getStrains() {
        return strains;
    }

    public String[] getStrainList() {
        return this.strainList;
    }
}
