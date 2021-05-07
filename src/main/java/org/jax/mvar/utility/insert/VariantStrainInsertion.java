package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;
import org.jax.mvar.utility.Config;

import java.io.*;
import java.sql.*;
import java.util.*;

public class VariantStrainInsertion {

    private final static String SELECT_STRAIN_IDS = "SELECT id from strain where name like ?";
    private final static String SELECT_COUNT = "SELECT COUNT(*) from genotype_temp;";
    private final static String SELECT_GENOTYPE_TEMP = "SELECT id, format, genotype_data FROM genotype_temp WHERE id BETWEEN ? AND ?";
    private final static String INSERT_GENOTYPE = "INSERT INTO genotype (format, data, variant_id, strain_id) VALUES (?, ?, ?, ?)";
    private final static String INSERT_VARIANT_STRAIN = "INSERT INTO variant_strain (variant_id, strain_id, genotype) VALUES (?, ?, ?)";

    /**
     * Insert variant/transcripts relationships given the variant_transcript_temp table
     *
     * @param batchSize
     * @param startId in case a process needs to be re-run from a certain variant_id (instead of starting from the beginning all over again
     * @param strainFilePath full path of strain file
     */
    public static void insertVariantStrainRelationships(int batchSize, int startId, String strainFilePath) {
        System.out.println("Inserting Variant Strain relationships...");
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // get Properties
        Config config = new Config();

        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {

            List<Integer> strainIds = getStrainIds(connection, strainFilePath);
            System.out.println();
            // count all genotypes data saved
            PreparedStatement countStmt = null;
            ResultSet resultCount = null;
            int numberOfRecords = 0;
            try {
                countStmt = connection.prepareStatement(SELECT_COUNT);
                resultCount = countStmt.executeQuery();
                if (resultCount.next()) {
                    numberOfRecords = resultCount.getInt(1);
                    System.out.println("NumberOfRows = " + numberOfRecords);
                } else {
                    System.out.println("error: could not get the record counts");
                }
            } finally {
                if (resultCount != null)
                    resultCount.close();
                if (countStmt != null)
                    countStmt.close();
            }
            System.out.println("Batch size is " + batchSize);
            VariantInsertion.innoDBSetOptions(connection, false);
            int selectIdx = startId;
            long start, elapsedTimeMillis;
            Map<Integer, String[]> variantIdGenotypeMap;
            for (int i = startId - 1; i < numberOfRecords; i++) {
                if (i > startId && i % batchSize == 0) {
                    start = System.currentTimeMillis();
                    variantIdGenotypeMap = selectGenotypeFromTemp(connection, selectIdx, selectIdx + batchSize - 1);

                    insertVariantStrainInBatch(connection, variantIdGenotypeMap, strainIds);
                    variantIdGenotypeMap.clear();
                    elapsedTimeMillis = System.currentTimeMillis() - start;
                    System.out.println("Progress: " + i + " of " + numberOfRecords + ", duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + (selectIdx + batchSize - 1));
                    selectIdx = selectIdx + batchSize;
                }
            }
            // last batch
            start = System.currentTimeMillis();
            variantIdGenotypeMap = selectGenotypeFromTemp(connection, selectIdx, numberOfRecords);
            if (variantIdGenotypeMap.size() > 0) {
                insertVariantStrainInBatch(connection, variantIdGenotypeMap, strainIds);
                variantIdGenotypeMap.clear();
                elapsedTimeMillis = System.currentTimeMillis() - start;
                System.out.println("Progress: 100%, duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + numberOfRecords);
            }
            // time
            VariantInsertion.innoDBSetOptions(connection, true);

            System.out.println("Variant/Strain relationships and genotype data inserted in " + stopWatch);
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }

    private static List<Integer> getStrainIds(Connection connection, String strainFilePath) throws SQLException {
        // Collect list of strains from strainFile
        // get the file names from a list a the chromosome file names is not in the wanted order
        final List<String> strains = new ArrayList<>();
        try {
            try (BufferedReader in = new BufferedReader(new FileReader(new File(strainFilePath)))) {
                String line = in.readLine(); // read a line at a time
                while (line != null) { // loop till you have no more lines
                    strains.add(line); // add the line to your list
                    line = in.readLine(); // try to read another line
                }
            }
        }catch (Exception exc) {
            System.out.println(exc.getMessage());
        }

        // collect the list of all the strain ids given the list of strain names for the Sanger data
        PreparedStatement strainIdStmt = null;
        ResultSet resultStrainIds = null;
        List<Integer> strainIds = new ArrayList<Integer>();
        String strStrainIds = "";
        for (int i = 0; i < strains.size(); i++) {
            try {
                strainIdStmt = connection.prepareStatement(SELECT_STRAIN_IDS);
                strainIdStmt.setString(1, strains.get(i) + "%");
                resultStrainIds = strainIdStmt.executeQuery();
                if (resultStrainIds.next()) {
                    int id = resultStrainIds.getInt("id");
                    strainIds.add(id);
                    strStrainIds = strStrainIds + ":" + id;
                }
            }  catch (SQLException exc) {
                throw exc;
            } finally {
                if (resultStrainIds != null)
                    resultStrainIds.close();
                if (strainIdStmt != null)
                    strainIdStmt.close();
            }
        }
        System.out.println("The list of strain Ids is the following : " + strStrainIds);
        return strainIds;
    }

    private static Map<Integer, String[]> selectGenotypeFromTemp(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
        Map<Integer, String[]> variantIdGenotypeMap = new HashMap();

        try {
            selectStmt = connection.prepareStatement(SELECT_GENOTYPE_TEMP);
            selectStmt.setInt(1, start);
            selectStmt.setInt(2, stop);
            result = selectStmt.executeQuery();
            while (result.next()) {
                // ... get column values from this record
                int variantId = result.getInt("id");
//                String format = result.getString("format");
                String[] genotypes = result.getString("genotype_data").split("\t");
//                variantIdGenotypeMap.put(variantId, new Genotype(format, genotypes));
                variantIdGenotypeMap.put(variantId, genotypes);
            }
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (result != null)
                result.close();
            if (selectStmt != null)
                selectStmt.close();
        }
        return variantIdGenotypeMap;
    }

    private static void insertVariantStrainInBatch(Connection connection, Map<Integer, String[]> variantIdGenotypeMap, List<Integer> strainIds) throws SQLException {
        // insert in variant transcript relationship
        PreparedStatement insertVariantStrain = null;

        try {
            insertVariantStrain = connection.prepareStatement(INSERT_VARIANT_STRAIN);

            connection.setAutoCommit(false);
            for (Map.Entry<Integer, String[]> entry : variantIdGenotypeMap.entrySet()) {
                int variantId = entry.getKey();
                String[] geno = entry.getValue();
                for (int i = 0; i < geno.length; i ++) {
                    // if there is a genotype data for the strain
//                  if (!geno[i].startsWith("./.") && !geno[i].startsWith("0/0")) {
                    insertVariantStrain.setInt(1, variantId);
                    insertVariantStrain.setInt(2, strainIds.get(i));
                    // we parse the first id (GT)
                    insertVariantStrain.setString(3, geno[i].split(":")[0]);
                    insertVariantStrain.addBatch();
                }
            }
            insertVariantStrain.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertVariantStrain != null)
                insertVariantStrain.close();
        }
    }

}
