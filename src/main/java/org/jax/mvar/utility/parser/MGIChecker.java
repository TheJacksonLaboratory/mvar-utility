package org.jax.mvar.utility.parser;

import org.apache.commons.lang3.time.StopWatch;
import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.model.Assembly;
import org.jax.mvar.utility.model.Variant;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

public class MGIChecker {

    /**
     * Loads a VCF file in the database
     *
     * @param vcfFile     VCF file
     * @param assembly    reference assembly (can be mm10 or mm39)
     */
    public void loadVCF(File vcfFile, Assembly assembly) {

        // get Properties
        Config config = new Config();
        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            final StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            // parse variants into a Map
            Map<String, Variant> variations = VcfParser.parseVcf(vcfFile, vcfFile, false, assembly);
            // query database for duplicates
            Map<Integer, Variant> result = queryDatabase(connection, variations);
            writeToFile(result);
            System.out.println(variations.keySet().size() + " variants searched in the Database in " + stopWatch + ", " + new Date());
            stopWatch.reset();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An exception was caught: " + e.getMessage());
        }
    }

    private Map<Integer, Variant> queryDatabase(Connection connection, Map<String, Variant> variations) throws SQLException {

        Map<Integer, Variant> foundVariants = new LinkedHashMap<>();
        for (String key : variations.keySet()) {
            Variant var = variations.get(key);
            PreparedStatement selectCanonicalIdStmt = null;
            String selectCanonical = "select id from variant_canon_identifier where variant_ref_txt=\"" + var.getVariantRefTxt() +"\";";
            try {
                selectCanonicalIdStmt = connection.prepareStatement(selectCanonical);
                ResultSet idResult = selectCanonicalIdStmt.executeQuery();
                if (idResult.next()) {
                    int id = idResult.getInt("id");
                    foundVariants.put(id, var);
                }
            } finally {
                if (selectCanonicalIdStmt != null)
                    selectCanonicalIdStmt.close();
            }
        }
        return foundVariants;
    }

    private void writeToFile(Map<Integer, Variant> results) {
        String currentPath = (new File(".")).getAbsolutePath();
        File file = new File(currentPath + "/mgi_duplicates.txt");
        FileWriter fr = null;
        BufferedWriter br = null;
        try {
            fr = new FileWriter(file);
            br = new BufferedWriter(fr);
            br.write("id\tvariant_ref_txt" + System.getProperty("line.separator"));
            for (Integer key : results.keySet()) {
                Variant var = results.get(key);
                br.write(key + "\t" + var.getVariantRefTxt() + System.getProperty("line.separator"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error writing MGI duplicates to file: " + e.getMessage());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
                System.out.println("MGI duplicates written to file:" + file.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
