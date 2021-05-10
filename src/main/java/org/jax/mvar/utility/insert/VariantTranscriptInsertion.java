package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;
import org.jax.mvar.utility.Config;

import java.sql.*;
import java.util.*;

public class VariantTranscriptInsertion {

    private final static String SELECT_COUNT = "SELECT COUNT(*) from variant_transcript_temp";
    private final static String SELECT_VARIANT_TRANSCRIPT_TEMP = "SELECT id, transcript_ids FROM variant_transcript_temp WHERE id BETWEEN ? AND ?";
    private final static String INSERT_VARIANT_TRANSCRIPTS = "INSERT INTO variant_transcript (variant_transcripts_id, transcript_id, most_pathogenic) VALUES (?,?,?)";
    private final static String SELECT_SOURCE_ID = "SELECT id from source WHERE name=?";
    private final static String INSERT_VARIANT_SOURCE = "INSERT INTO variant_source (variant_sources_id, source_id) VALUES (?,?)";

    /**
     * Insert variant/transcripts relationships given the variant_transcript_temp table
     *
     * @param batchSize
     * @param startId in case a process needs to be re-run from a certain variant_id (instead of starting from the beginning all over again
     * @param sourceName
     */
    public static void insertVariantTranscriptRelationships(int batchSize, int startId, String sourceName) {
        System.out.println("Inserting Variant Transcript relationships...");
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // get Properties
        Config config = new Config();

        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            PreparedStatement countStmt = null, selectSourceIdStmt = null;
            ResultSet resultCount = null, sourceIdResult = null;
            int numberOfRecords = 0;
            int sourceId = 0;
            try {
                // get Source id
                selectSourceIdStmt = connection.prepareStatement(SELECT_SOURCE_ID);
                selectSourceIdStmt.setString(1, sourceName);
                sourceIdResult = selectSourceIdStmt.executeQuery();
                if (sourceIdResult.next()){
                    sourceId = sourceIdResult.getInt(1);
                }  else {
                    System.out.println("error: could not get the source id");
                }
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
                if (sourceIdResult != null)
                    sourceIdResult.close();
                if (selectSourceIdStmt != null)
                    selectSourceIdStmt.close();
            }
            System.out.println("Batch size is " + batchSize);
            connection.setAutoCommit(false);
            int selectIdx = startId;
            long start, elapsedTimeMillis;
            Map<Long, Set<Long>> variantIdTranscriptIdsMap;
            for (int i = startId - 1; i < numberOfRecords; i++) {
                if (i > startId && i % batchSize == 0) {
                    start = System.currentTimeMillis();
                    variantIdTranscriptIdsMap = selectVariantTranscriptsFromTemp(connection, selectIdx, selectIdx + batchSize - 1);

                    insertVariantTranscriptInBatch(connection, variantIdTranscriptIdsMap, sourceId);
                    variantIdTranscriptIdsMap.clear();
                    elapsedTimeMillis = System.currentTimeMillis() - start;
                    System.out.println("Progress: " + i + " of " + numberOfRecords + ", duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + (selectIdx + batchSize - 1));
                    selectIdx = selectIdx + batchSize;
                }
            }
            // last batch
            start = System.currentTimeMillis();
            variantIdTranscriptIdsMap = selectVariantTranscriptsFromTemp(connection, selectIdx, numberOfRecords);
            if (variantIdTranscriptIdsMap.size() > 0) {
                insertVariantTranscriptInBatch(connection, variantIdTranscriptIdsMap, sourceId);
                variantIdTranscriptIdsMap.clear();
                elapsedTimeMillis = System.currentTimeMillis() - start;
                System.out.println("Progress: 100%, duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + numberOfRecords);
            }
            // time
            System.out.println("Variant/Transcripts relationships inserted in " + stopWatch);
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }

    private static Map<Long, Set<Long>> selectVariantTranscriptsFromTemp(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
        Map<Long, Set<Long>> variantIdTranscriptIdsMap = new HashMap();

        try {
            selectStmt = connection.prepareStatement(SELECT_VARIANT_TRANSCRIPT_TEMP);
            selectStmt.setInt(1, start);
            selectStmt.setInt(2, stop);
            result = selectStmt.executeQuery();
            while (result.next()) {
                // ... get column values from this record
                long variantId = result.getLong("id");
                String[] transcripts = result.getString("transcript_ids").split(",");
                Set<Long> transcriptIdsSet = new LinkedHashSet<>();
                for (String transcriptId : transcripts) {
                    if (!transcriptId.equals("null") && !transcriptId.equals("0"))
                        transcriptIdsSet.add(Long.valueOf(transcriptId));
                }
                variantIdTranscriptIdsMap.put(variantId, transcriptIdsSet);
            }
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (result != null)
                result.close();
            if (selectStmt != null)
                selectStmt.close();
        }
        return variantIdTranscriptIdsMap;
    }

    private static void insertVariantTranscriptInBatch(Connection connection, Map<Long, Set<Long>> variantIdTranscriptIdsMap, int sourceId) throws SQLException {
        // insert in variant transcript relationship
        PreparedStatement insertVariantTranscripts = null, insertVariantSources = null;

        try {
            insertVariantTranscripts = connection.prepareStatement(INSERT_VARIANT_TRANSCRIPTS);
            insertVariantSources = connection.prepareStatement(INSERT_VARIANT_SOURCE);

            connection.setAutoCommit(false);
            for (Map.Entry<Long, Set<Long>> entry : variantIdTranscriptIdsMap.entrySet()) {
                long variantId = entry.getKey();
                // insert variant transcript relationship
                Set<Long> transcriptIds = entry.getValue();
                Iterator<Long> itr = transcriptIds.iterator();
                int idx = 0;
                while (itr.hasNext()) {
                    insertVariantTranscripts.setLong(1, variantId);
                    insertVariantTranscripts.setLong(2, itr.next());
                    insertVariantTranscripts.setBoolean(3, idx == 0);
                    insertVariantTranscripts.addBatch();
                    idx++;
                }
                // insert variant source relationship
                insertVariantSources.setLong(1, variantId);
                insertVariantSources.setLong(2, sourceId);
                insertVariantSources.addBatch();
            }
            insertVariantTranscripts.executeBatch();
            insertVariantSources.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertVariantTranscripts != null)
                insertVariantTranscripts.close();
            if (insertVariantSources != null)
                insertVariantSources.close();
        }
    }
}
