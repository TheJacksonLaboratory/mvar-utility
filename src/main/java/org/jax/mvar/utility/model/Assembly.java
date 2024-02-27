package org.jax.mvar.utility.model;

public enum Assembly {
    MM9 ("GRCm37"),
    MM10 ("GRCm38"),
    MM39 ("GRCm39");

    public final String label;

    private Assembly(String label) {
        this.label = label;
    }

}
