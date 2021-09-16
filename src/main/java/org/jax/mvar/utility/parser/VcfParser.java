package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.model.Variant;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

/**
 * Class used to parse a VCF file line by line
 * 
 *
 */
public class VcfParser {

    /**
     * Parse a VCF file. If checkForCanon is true, a batch search for canonicals in the MVAR DB will be done
     * and the result HashMap of variations will only contain variants that are not found in the DB.
     * @param vcfFile
     * @param headerFile If one input file, headerFile can be the same as the vcfFile.
     *                   If multiple, and only the first file has a header, you need to add a header file, so that the annotations
     *                   can be found and used for the insertion.
     * @param checkForCanon
     * @return
     */
    public static Map<String, Variant> parseVcf(File vcfFile, File headerFile, boolean checkForCanon) throws Exception {
        Map<String, Variant> variations;

        if (vcfFile.getName().endsWith(".vcf")) {
            // read file line by line of unzipped file
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                // create new input stream reader
                InputStreamReader instrm = new InputStreamReader(is);
                // Create the object of BufferedReader object
                BufferedReader br = new BufferedReader(instrm)
            ) {
                InfoParser infoParser = new ConsequenceParser(headerFile);
                variations = parse(vcfFile.getName(), br, infoParser, checkForCanon);
            }
        } else {
            // gzipped read line by line
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                InputStream gzipStream = new GZIPInputStream(is);
                Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(decoder)
            ) {
                InfoParser vepParser = new ConsequenceParser(headerFile);
                variations = parse(vcfFile.getName(), br, vepParser,checkForCanon);
            }
        }

        return variations;
    }

    private static Map<String, Variant> parse(String filename, BufferedReader br, InfoParser infoParser, boolean checkForCanon) throws Exception {
        Map<String, Variant> variations = new LinkedHashMap<>();

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
                List<String> rsIdAndHgvs = ((ConsequenceParser)infoParser).getRsIDAndHGVS(jannotationAndCSQ.get("CSQ"));
                List<Map<String, String>> csqAnnotations = infoParser.parse(jannotationAndCSQ.get("CSQ"));
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
                            columns[4], columns[5], columns[6], columns[8], rsIdAndHgvs.get(1), csqAnnotations.get(0).get("Protein_position"), csqAnnotations.get(0).get("Amino_acids"), jannotationAndCSQ.get("ANN"), genotypeData);
                } else {
                    var = new Variant(columns[0], columns[1], rsId, columns[3],
                            columns[4], columns[5], columns[6], "", rsIdAndHgvs.get(1), csqAnnotations.get(0).get("Protein_position"), csqAnnotations.get(0).get("Amino_acids"), jannotationAndCSQ.get("ANN"), null);
                }
                if (variations.containsKey(var.getVariantRefTxt()))
                    System.out.println(var.getVariantRefTxt() + " already exists and will be overridden.");
                variations.put(var.getVariantRefTxt(), var);
                idx++;
            }
        }
        System.out.println(idx + " variants parsed from " + filename);

        if (checkForCanon) {
            System.out.println("Looking for Canonicals...");
            PreparedStatement selectStmt = null;
            ResultSet result = null;
            Config config = new Config();

            String variantQuery = "select id, variant_ref_txt from variant where variant_ref_txt in(";
            StringBuilder sql = new StringBuilder();
            sql.append(variantQuery);
            for (int i = 0; i < variations.size(); i++) {
                sql.append("?");
                if(i+1 < variations.size()){
                    sql.append(",");
                }
            }
            sql.append(")");

            try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
                int index = 0;
                selectStmt = connection.prepareStatement(sql.toString());

                for (Map.Entry<String, Variant> entry : variations.entrySet()) {
                    String key = entry.getKey();
                    Variant variant = entry.getValue();
                    selectStmt.setString(index+1, variant.getVariantRefTxt());
                    index++;
                }
                result = selectStmt.executeQuery();

                int myIdx = 0;
                while (result.next()) {
                    int variantId = result.getInt("id");
                    String variantRefTxt = result.getString("variant_ref_txt");

                    Variant var = variations.get(variantRefTxt);
                    if (var != null) {
                        var.setExists(true);
                        var.setExistingId(variantId);
                        myIdx++;
                    }
                }
                System.out.println(myIdx + " variants were found.");

            } catch (SQLException exc) {
                throw exc;
            } finally {
                if (result != null)
                    result.close();
                if (selectStmt != null)
                    selectStmt.close();
            }
        }
        return variations;
    }
}
