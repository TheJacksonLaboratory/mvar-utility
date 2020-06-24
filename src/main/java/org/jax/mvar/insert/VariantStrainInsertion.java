package org.jax.mvar.insert;

import org.apache.commons.lang3.time.StopWatch;

import java.sql.*;
import java.util.*;

public class VariantStrainInsertion {

    private final static String SELECT_COUNT = "SELECT COUNT(*) from genotype_temp;";
    private final static String SELECT_GENOTYPE_TEMP = "SELECT id, format, genotype_data FROM genotype_temp WHERE id BETWEEN ? AND ?";
    private final static String INSERT_GENOTYPE = "INSERT INTO genotype (format, data, variant_id, strain_id) VALUES (?, ?, ?, ?)";
    private final static String INSERT_VARIANT_STRAIN = "INSERT INTO variant_strain (variant_strains_id, strain_id) VALUES (?, ?)";
    private final static int[] STRAIN_IDS = { 283, 2, 661, 4, 3, 5, 6729, 1236, 179, 6, 1569, 8, 10119, 199, 1902, 9, 10, 68, 11, 12, 2184, 43541, 7568, 9972, 862, 13, 6838, 14, 867, 15, 2008, 1214, 16, 48883, 48884, 888, 2362, 896, 863, 19, 2361, 17, 31346 };
    private final static String[] STRAIN_LIST = {"129P2/OlaHsd", "129S1/SvImJ", "129S5/SvEvBrd", "AKR/J", "A/J", "BALB/cJ", "BTBR T<+> Itpr3<tf>/J", "BUB/BnJ", "C3H/HeH", "C3H/HeJ", "C57BL/10J", "C57BL/6NJ", "C57BR/cdJ", "C57L/J", "C58/J", "CAST/EiJ", "CBA/J", "DBA/1J", "DBA/2J", "FVB/NJ", "I/LnJ", "JF1/MsJ", "KK/HlJ", "LEWES/EiJ", "LG/J", "LP/J", "MOLF/EiJ", "NOD/ShiLtJ", "NZB/BlNJ", "NZO/HlLtJ", "NZW/LacJ", "PL/J", "PWK/PhJ", "QSi3", "QSi5", "RF/J", "SEA/GnJ", "SJL/J", "SM/J", "SPRET/EiJ", "ST/bJ", "WSB/EiJ", "ZALENDE/EiJ" };


    /**
     * Insert variant/transcripts relationships given the variant_transcript_temp table
     *
     * @param batchSize
     * @param startId in case a process needs to be re-run from a certain variant_id (instead of starting from the beginning all over again
     */
    public static void insertVariantStrainRelationships(int batchSize, int startId) {
        System.out.println("Inserting Variant Strain relationships...");
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // get Properties
        Config config = new Config();

        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
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

                    insertVariantStrainInBatch(connection, variantIdGenotypeMap);
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
                insertVariantStrainInBatch(connection, variantIdGenotypeMap);
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

    private static Map<Integer, String[]> selectGenotypeFromTemp(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
//        Map<Integer, Genotype> variantIdGenotypeMap = new HashMap();
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

    private static void insertVariantStrainInBatch(Connection connection, Map<Integer, String[]> variantIdGenotypeMap) throws SQLException {
        // insert in variant transcript relationship
        PreparedStatement insertVariantStrain = null;
//        PreparedStatement insertGenotype = null;

        try {
            insertVariantStrain = connection.prepareStatement(INSERT_VARIANT_STRAIN);
//            insertGenotype = connection.prepareStatement(INSERT_GENOTYPE);

            connection.setAutoCommit(false);
            for (Map.Entry<Integer, String[]> entry : variantIdGenotypeMap.entrySet()) {
                int variantId = entry.getKey();
                String[] geno = entry.getValue();
//                String format = geno.getFormat();
//                String[] genotypeData = geno.getGenotype();
                for (int i = 0; i < geno.length; i ++) {
                    // if there is a genotype data for the strain
                    if (!geno[i].startsWith("./.")) {
//                        insertGenotype.setString(1, format);
//                        insertGenotype.setString(2, genotypeData[i]);
//                        insertGenotype.setInt(3, variantId);
//                        insertGenotype.setInt(4, STRAIN_IDS[i]);
//                        insertGenotype.addBatch();
                        insertVariantStrain.setInt(1, variantId);
                        insertVariantStrain.setInt(2, STRAIN_IDS[i]);
                        insertVariantStrain.addBatch();
                    }
                }
            }
//            insertGenotype.executeBatch();
            insertVariantStrain.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
//            if (insertGenotype != null)
//                insertGenotype.close();
            if (insertVariantStrain != null)
                insertVariantStrain.close();
        }
    }

}
