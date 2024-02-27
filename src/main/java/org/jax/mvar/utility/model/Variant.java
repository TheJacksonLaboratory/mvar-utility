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
    String originalRefText;
    String proteinPosition;
    String aminoAcidChange;
    Map strains;
    String[] strainList;
    boolean exists;
    int existingId;

    public Variant(String chr, String pos, String id, String ref, String alt, String qual, String filter,
                   String format, String hgvsg, String proteinPosition, String aminoAcidChange,
                   String jannovarAnnotation, String genotypeData) {
        this.chr = chr.replace("ch", "").replace("r", "");
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.format = format;
        this.hgvsg = hgvsg;
        this.proteinPosition = proteinPosition;
        this.aminoAcidChange = aminoAcidChange;
        this.jannovarAnnotation = jannovarAnnotation;
        this.genotypeData = genotypeData;
        // default value set to false
        this.exists = false;
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

    public String getJannovarAnnotation() {
        return jannovarAnnotation;
    }

    public String getVariantRefTxt() {
        return variantRefTxt;
    }
    public String getOriginalRefTxt() {
        return originalRefText;
    }

    /**
     * Sets the original VariantRefTxt given the original alleles(an array with two items, one for ref and one for alt)
     * and original position
     * @param originalAlleles
     * @param originalPosition
     */
    public void setOriginalRefText(String originalAlleles, String originalPosition) {
        String ref = "", alt = "";
        if (originalAlleles != null && !originalAlleles.equals("")) {
            ref = originalAlleles.split(",")[0];
            alt = originalAlleles.split(",")[1];
        } else {
            ref = this.ref;
            alt = this.alt;
        }
        this.originalRefText = chr.concat("_").concat(originalPosition).concat("_").concat(ref).concat("_").concat(alt);
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

    public void setExists(boolean exists) {
        this.exists = exists;
    }

    public boolean getExists() {
        return this.exists;
    }

    public void setExistingId(int id) {
        this.existingId = id;
    }

    public int getExistingId() {
        return this.existingId;
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

    public String getProteinPosition() { return this.proteinPosition; }

    public String getAminoAcidChange() { return this.aminoAcidChange; }
}
