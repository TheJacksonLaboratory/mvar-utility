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
    String info;
    String format;
    String genotypeData;
    String variantHgvsNotation;
    String type;
    String annotation;
    String variantRefTxt;
    Map strains;
    String[] strainList;

    public Variant(String chr, String pos, String id, String ref, String alt, String qual,
                   String filter, String info, String format, String genotypeData, String variantHgvsNotation,
                   String[] strainList) throws Exception {
        this.chr = chr.replace("ch", "").replace("r", "");
        this.pos = pos;
        this.id = id;
        this.ref = ref;
        this.alt = alt;
        this.qual = qual;
        this.filter = filter;
        this.info = info;
        this.format = format;
        this.genotypeData = genotypeData;
        this.annotation = getAnnotation(this.info.split(";"), "ANN");
        this.variantRefTxt = chr.concat("_").concat(pos).concat("_").concat(ref).concat("_").concat(alt);
        setType(ref, alt);
        this.variantHgvsNotation = variantHgvsNotation;
        this.strainList = strainList;
//        setStrains(genotypeData, strainList);
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

    public String getHGVS() { return variantHgvsNotation; }

    public String getQual() { return qual; }

    public String getFilter() { return filter; }

    public String getInfo() { return info; }

    public String getFormat() { return format; }

    public String getGenotypeData() { return genotypeData; }

    private String getAnnotation(String[] annotations, String id) {
        String annotation;
        for (int i = 0; i < annotations.length; i++) {
            if (annotations[i].startsWith(id)) {
                annotation = annotations[i];
                return annotation;
            }
        }
        return "";
    }

    public String getAnnotation() {
        return annotation;
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
