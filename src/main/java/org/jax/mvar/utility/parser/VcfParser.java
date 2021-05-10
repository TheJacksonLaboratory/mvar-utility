package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.model.Variant;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Class used to parse a VCF file line by line
 * 
 *
 */
public class VcfParser {

    /**
     * Parse a VCF file given a chromosome and optionally a type.
     * @param vcfFile
     * @return
     */
    public static LinkedHashMap<String, Variant> parseVcf(File vcfFile) throws Exception {
        LinkedHashMap<String, Variant> variations;

        if (vcfFile.getName().endsWith(".vcf")) {
            // read file line by line of unzipped file
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                // create new input stream reader
                InputStreamReader instrm = new InputStreamReader(is);
                // Create the object of BufferedReader object
                BufferedReader br = new BufferedReader(instrm)
            ) {
                variations = parse(vcfFile.getName(), br);
            }
        } else {
            // gzipped read line by line
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                InputStream gzipStream = new GZIPInputStream(is);
                Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(decoder)
            ) {
                variations = parse(vcfFile.getName(), br);
            }
        }

        return variations;
    }

    private static LinkedHashMap<String, Variant> parse(String filename, BufferedReader br) throws Exception {
        LinkedHashMap<String, Variant> variations = new LinkedHashMap<>();

        String next, strLine = br.readLine();
        int idx = 0;
        // load data in to map
        for (boolean last = (strLine == null); !last; strLine = next) {
            last = ((next = br.readLine()) == null);
            if (!strLine.startsWith("#")) {
                String[] columns = strLine.split("\t");

                // jannovar transcript annotation and VEP annotation
                Map<String, String> jannotationAndCSQ = InfoParser.getANNandCSQ(columns[7].split(";"));
                // VEP hgvs annotation
                List<String> rsIdAndHgvs = ConsequenceParser.getRsIDAndHGVS(jannotationAndCSQ.get("CSQ"));
                String rsId;
                // rsId
                if (columns[2].isEmpty() || columns[2].equals(".")) {
                    rsId = rsIdAndHgvs != null ? rsIdAndHgvs.get(0) : columns[2];
                } else {
                    rsId = columns[2];
                }
                Variant var;
                if (columns.length > 7) {
                    String[] genotypes = Arrays.copyOfRange(columns, 9, columns.length);
                    String genotypeData = String.join("\t", genotypes);
                    var = new Variant(columns[0], columns[1], rsId, columns[3],
                            columns[4], columns[5], columns[6], columns[8], rsIdAndHgvs.get(1), jannotationAndCSQ.get("ANN"), genotypeData);
                } else {
                    var = new Variant(columns[0], columns[1], rsId, columns[3],
                            columns[4], columns[5], columns[6], "", rsIdAndHgvs.get(1), jannotationAndCSQ.get("ANN"), null);
                }
                if (variations.containsKey(var.getVariantRefTxt()))
                    System.out.println(var.getVariantRefTxt() + " already exists and will be overridden.");
                variations.put(var.getVariantRefTxt(), var);
                idx++;
            }
        }
        System.out.println(idx + " variants parsed from " + filename);

        return variations;
    }
}
