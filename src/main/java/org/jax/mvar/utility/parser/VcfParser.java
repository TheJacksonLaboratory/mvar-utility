package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.model.Variant;

import java.io.*;
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
                BufferedReader br = new BufferedReader(instrm);
            ) {
                variations = parse(vcfFile.getName(), br);
            }
        } else {
            // gzipped read line by line
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                InputStream gzipStream = new GZIPInputStream(is);
                Reader decoder = new InputStreamReader(gzipStream, "UTF-8");
                BufferedReader br = new BufferedReader(decoder);
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

                // jannovar transcript annotation
                String jannotation = InfoParser.getAnnotation(columns[7].split(";"), "ANN");
                // VEP hgvs annotation
                String hgvsg = ConsequenceParser.getCSQ(InfoParser.getAnnotation(columns[7].split(";"), "CSQ"), "HGVSg");
                String rsId;
                // rsId
                if ((columns[2] == null)||(columns[2] != null && (columns[2].isEmpty() || columns[2].equals(".")))) {
                    rsId = ConsequenceParser.getCSQ(InfoParser.getAnnotation(columns[7].split(";"), "CSQ"), "Existing_variation");
                } else {
                    rsId = columns[2];
                }
                Variant var;
                if (columns.length > 8) {
                    String[] genotypes = Arrays.copyOfRange(columns, 9, columns.length);
                    String genotypeData = String.join("\t", genotypes);
                    var = new Variant(columns[0], columns[1], rsId, columns[3],
                            columns[4], columns[5], columns[6], columns[8], hgvsg, jannotation, genotypeData);
                } else {
                    var = new Variant(columns[0], columns[1], rsId, columns[3],
                            columns[4], columns[5], columns[6], columns[8], hgvsg, jannotation, null);
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
