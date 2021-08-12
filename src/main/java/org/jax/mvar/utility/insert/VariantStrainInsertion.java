package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;
import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.parser.ParserUtils;

import java.io.*;
import java.sql.*;
import java.util.*;

public class VariantStrainInsertion {
    private static int numberOfRecords = 0;

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

            List<Integer> strainIds = ParserUtils.getStrainIds(connection, new File(strainFilePath));

            if (numberOfRecords == 0) {
                // count all genotypes data saved
                PreparedStatement countStmt = null;
                ResultSet resultCount = null;
                try {
                    countStmt = connection.prepareStatement("SELECT COUNT(*) from genotype_temp;");
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
            } else {
                System.out.println("NumberOfRows = " + numberOfRecords);
            }

            System.out.println("Batch size is " + batchSize);
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

            System.out.println("Variant/Strain relationships and genotype data inserted in " + stopWatch);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    private static Map<Integer, String[]> selectGenotypeFromTemp(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
        Map<Integer, String[]> variantIdGenotypeMap = new HashMap();

        try {
            selectStmt = connection.prepareStatement("SELECT id, variant_id, format, genotype_data FROM genotype_temp WHERE id BETWEEN ? AND ?");
            selectStmt.setInt(1, start);
            selectStmt.setInt(2, stop);
            result = selectStmt.executeQuery();
            while (result.next()) {
                // ... get column values from this record
                int variantId = result.getInt("variant_id");
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
            insertVariantStrain = connection.prepareStatement("INSERT INTO variant_strain (variant_id, strain_id, genotype, imputed) VALUES (?, ?, ?, ?)");
            VariantInsertion.innoDBSetOptions(connection, false);
            connection.setAutoCommit(false);
            for (Map.Entry<Integer, String[]> entry : variantIdGenotypeMap.entrySet()) {
                int variantId = entry.getKey();
                String[] geno = entry.getValue();
                for (int i = 0; i < geno.length; i++) {
                    // if there is a genotype data for the strain
                    // we don't add the entry if 0/0
                    if (!geno[i].startsWith("0/0")) {
                        insertVariantStrain.setInt(1, variantId);
                        insertVariantStrain.setInt(2, strainIds.get(i));
                        // we parse the first id (GT)
                        int idx = geno[i].indexOf(':');
                        String gtValue = geno[i].substring(0, idx);
                        insertVariantStrain.setString(3, gtValue);
                        insertVariantStrain.setByte(4, (byte)0); // default non imputed is 0
                        insertVariantStrain.addBatch();
                    }
                }
            }
            insertVariantStrain.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertVariantStrain != null)
                insertVariantStrain.close();
            VariantInsertion.innoDBSetOptions(connection, true);
            connection.setAutoCommit(true);
        }
    }

}
