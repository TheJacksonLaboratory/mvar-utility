package org.jax.mvar.utility.parser;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
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

    public static final String VARIANT_SELECT_CANON = "select variant_ref_txt from variant where variant_ref_txt=?";
    /**
     * Parse a VCF file. If checkForCanon is true, a batch search for canonicals in the MVAR DB will be done
     * and the result HashMap of variations will only contain variants that are not found in the DB.
     * @param vcfFile
     * @param checkForCanon
     * @return
     */
    public static LinkedHashMap<String, Variant> parseVcf(File vcfFile, boolean checkForCanon) throws Exception {
        LinkedHashMap<String, Variant> variations;

        if (vcfFile.getName().endsWith(".vcf")) {
            // read file line by line of unzipped file
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                // create new input stream reader
                InputStreamReader instrm = new InputStreamReader(is);
                // Create the object of BufferedReader object
                BufferedReader br = new BufferedReader(instrm)
            ) {
                variations = parse(vcfFile.getName(), br, checkForCanon);
            }
        } else {
            // gzipped read line by line
            try(InputStream is = new FileInputStream(vcfFile.getPath());
                InputStream gzipStream = new GZIPInputStream(is);
                Reader decoder = new InputStreamReader(gzipStream, StandardCharsets.UTF_8);
                BufferedReader br = new BufferedReader(decoder)
            ) {
                variations = parse(vcfFile.getName(), br, checkForCanon);
            }
        }

        return variations;
    }

    private static LinkedHashMap<String, Variant> parse(String filename, BufferedReader br, boolean checkForCanon) throws Exception {
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

        if (checkForCanon) {
            // TODO finalize the following code. For now this is temporarily not functional. And checkForCanon is always false.
            //  check for existing canonicals in the DB and remove existing ones from the variation map
//            PreparedStatement selectStmt = null;
//            ResultSet result = null;
//            Config config = new Config();
//            try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
//                List<String> keysToRemove;
//                int index = 0;
//                selectStmt = connection.prepareStatement(VARIANT_SELECT_CANON);
//                for (Map.Entry<String, Variant> entry : variations.entrySet()) {
//                    String key = entry.getKey();
//                    Variant value = entry.getValue();
//
//
//                    if (index > 1 && index % batchSize == 0) {
//
//                        selectStmt.setString(1, "");
//                        selectStmt.addBatch();
//                    }
//                    index++;
//                }
//                //last batch
//                if (variations.size() > 0) {
//                    batchInsertVariantsJDBC2(connection, batchOfVars, batchOfGenes, batchOfTranscripts, canonIdx);
//                    batchOfVars.clear();
//                    batchOfGenes.clear();
//                    batchOfTranscripts.clear();
//                }
//                
//                selectStmt = connection.prepareStatement(VARIANT_SELECT_CANON);
//                selectStmt.setString(1, "");
//                selectStmt.addBatch();
//
//            } catch (SQLException exc) {
//                throw exc;
//            } finally {
//                if (result != null)
//                    result.close();
//                if (selectStmt != null)
//                    selectStmt.close();
//            }
        }
        return variations;
    }
}
