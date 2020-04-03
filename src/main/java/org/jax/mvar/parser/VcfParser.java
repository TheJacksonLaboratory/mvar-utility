package org.jax.mvar.parser;

import org.jax.mvar.insert.Variant;

import java.io.*;
import java.util.Arrays;
import java.util.LinkedHashMap;

/**
 * Class that uses the gngs.VCF parser
 * 
 *
 */
public class VcfParser {

    /**
     * Parse a VCF file given a chromosome and optionally a type.
     * @param vcfFile
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
//            while((strLine = br.readLine()) != null) {
            String next, strLine = br.readLine();
            int idx = 0;
            // load data in to map
            for (boolean last = (strLine == null); !last; strLine = next) {
                last = ((next = br.readLine()) == null);
                if (!strLine.startsWith("#")) {
                    String[] columns = strLine.split("\t");
                    String[] genotypes = Arrays.copyOfRange(columns, 9, columns.length);
                    String genotypeData = String.join("\t", genotypes);
                    Variant var = new Variant(columns[0], columns[1], columns[2], columns[3],
                            columns[4], columns[5], columns[6], columns[7], columns[8], genotypeData, strainList);
                    variations.put(var.getVariantRefTxt(), var);
                    idx++;
                }
            }
            System.out.println(idx + " variants parsed from " + vcfFile.getName());
        }
        return variations;
    }

}
