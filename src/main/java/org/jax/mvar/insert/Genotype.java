package org.jax.mvar.insert;

public class Genotype {
    private String format;
    private String[] genotypes;

    public Genotype(String format, String[] genotypeData) {
        this.format = format;
        this.genotypes = genotypeData;
    }
    public String getFormat() {
        return format;
    }

    public String[] getGenotype() {
        return genotypes;
    }


}
