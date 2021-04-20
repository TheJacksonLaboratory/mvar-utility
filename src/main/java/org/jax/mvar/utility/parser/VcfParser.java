package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.model.Variant;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Class used to parse a VCF file line by line
 * 
 *
 */
public class VcfParser {

    /**
     * Parse a VCF file given a chromosome and optionally a type.
     * @param vcfFile
     * @param strainList
     * @return
     */
    public static LinkedHashMap<String, Variant> parseVcf(File vcfFile, String[] strainList) throws Exception {
        LinkedHashMap<String, Variant> variations = new LinkedHashMap<>();

        // read file line by line
        try(InputStream is = new FileInputStream(vcfFile.getPath());
            // create new input stream reader
            InputStreamReader instrm = new InputStreamReader(is);
            // Create the object of BufferedReader object
            BufferedReader br = new BufferedReader(instrm);
        ) {
            String next, strLine = br.readLine();
            int idx = 0;
            // load data in to map
            for (boolean last = (strLine == null); !last; strLine = next) {
                last = ((next = br.readLine()) == null);
                if (!strLine.startsWith("#")) {
                    String[] columns = strLine.split("\t");
                    Variant var;
                    if (columns.length > 8) {
                        String[] genotypes = Arrays.copyOfRange(columns, 9, columns.length);
                        String genotypeData = String.join("\t", genotypes);
                        var = new Variant(columns[0], columns[1], columns[2], columns[3],
                                columns[4], columns[5], columns[6], columns[7], columns[8], genotypeData, null);
                    } else {
                        var = new Variant(columns[0], columns[1], columns[2], columns[3],
                                columns[4], columns[5], columns[6], columns[7], null, null, null);
                    }
                    if (variations.containsKey(var.getVariantRefTxt()))
                        System.out.println(var.getVariantRefTxt() + " already exists and will be overridden.");
                    variations.put(var.getVariantRefTxt(), var);
                    idx++;
                }
            }
            System.out.println(idx + " variants parsed from " + vcfFile.getName());
        }
        return variations;
    }

}
