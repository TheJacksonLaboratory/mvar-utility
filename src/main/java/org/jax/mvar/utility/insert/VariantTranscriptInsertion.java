package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;
import org.jax.mvar.utility.Config;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class VariantTranscriptInsertion {


    /**
     * Insert variant/transcripts and variant/source relationships given the variant_transcript_temp table and the source Name
     *
     * @param batchSize
     * @param startId in case a process needs to be re-run from a certain variant_id (instead of starting from the beginning all over again
     * @param sourceName
     * @throws Exception
     */
    public static void insertVariantTranscriptSourceRel(int batchSize, int startId, String sourceName) throws Exception {
        System.out.println("Inserting Variant Transcript relationships, " + new Date());
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        // get Properties
        Config config = new Config();

        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            int numberOfRecords = getCount(connection);
            int sourceId = getSourceId(connection, sourceName);
            System.out.println("Batch size is " + batchSize);
            connection.setAutoCommit(false);
            int selectIdx = startId;
            long start, elapsedTimeMillis;
            Map<Long, Set<Long>> variantIdTranscriptIdsMap;
            for (int i = startId - 1; i < numberOfRecords; i++) {
                if (i > startId && i % batchSize == 0) {
                    start = System.currentTimeMillis();
                    variantIdTranscriptIdsMap = selectVariantTranscriptsFromTemp(connection, selectIdx, selectIdx + batchSize - 1);

                    insertVariantTranscriptSourceInBatch(connection, variantIdTranscriptIdsMap, sourceId);
                    variantIdTranscriptIdsMap.clear();
                    elapsedTimeMillis = System.currentTimeMillis() - start;
                    System.out.println("Progress: " + i + " of " + numberOfRecords + ", duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + (selectIdx + batchSize - 1) + ". " + new Date());
                    selectIdx = selectIdx + batchSize;
                }
            }
            // last batch
            start = System.currentTimeMillis();
            variantIdTranscriptIdsMap = selectVariantTranscriptsFromTemp(connection, selectIdx, numberOfRecords);
            if (variantIdTranscriptIdsMap.size() > 0) {
                insertVariantTranscriptSourceInBatch(connection, variantIdTranscriptIdsMap, sourceId);
                variantIdTranscriptIdsMap.clear();
                elapsedTimeMillis = System.currentTimeMillis() - start;
                System.out.println("Progress: 100%, duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items inserted: " + selectIdx + " to " + numberOfRecords + ". " + new Date());
            }
            // time
            System.out.println("Variant/Transcripts relationships inserted in " + stopWatch);
        } catch (SQLException exc) {
            exc.printStackTrace();
        }
    }

    private static int getSourceId(Connection connection, String sourceName) throws Exception {
        PreparedStatement selectSourceIdStmt = null;
        ResultSet sourceIdResult = null;
        int sourceId = 0;
        try {
            // get Source id
            selectSourceIdStmt = connection.prepareStatement("SELECT id from source WHERE name=?");
            selectSourceIdStmt.setString(1, sourceName);
            sourceIdResult = selectSourceIdStmt.executeQuery();
            if (sourceIdResult.next()){
                sourceId = sourceIdResult.getInt(1);
                // if the value returned is 0, it means the result was a SQL NULL
                if (sourceId == 0)
                    throw new Exception("Error: the source Name given couldn't be found in the database. " +
                            "Please add the new source name to the database in order to use this source.");
            }  else {
                System.out.println("error: could not get the source id");
            }
        } finally {
            if (sourceIdResult != null)
                sourceIdResult.close();
            if (selectSourceIdStmt != null)
                selectSourceIdStmt.close();
        }
        return sourceId;
    }

    private static int getCount(Connection connection) throws SQLException {
        PreparedStatement countStmt = null;
        ResultSet resultCount = null;
        int numberOfRecords = 0;
        try {
            countStmt = connection.prepareStatement("SELECT COUNT(*) from variant_transcript_temp");
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
        return numberOfRecords;
    }

    private static Map<Long, Set<Long>> selectVariantTranscriptsFromTemp(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
        Map<Long, Set<Long>> variantIdTranscriptIdsMap = new LinkedHashMap<>();

        try {
            selectStmt = connection.prepareStatement("SELECT id, transcript_ids FROM variant_transcript_temp WHERE id BETWEEN ? AND ?");
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

    private static void insertVariantTranscriptSourceInBatch(Connection connection, Map<Long, Set<Long>> variantIdTranscriptIdsMap, int sourceId) throws SQLException {
        // insert in variant transcript relationship
        PreparedStatement insertVariantTranscripts = null, insertVariantSources = null;

        try {
            insertVariantTranscripts = connection.prepareStatement("INSERT INTO variant_transcript (variant_transcripts_id, transcript_id, most_pathogenic) VALUES (?,?,?)");
            insertVariantSources = connection.prepareStatement("INSERT INTO variant_source (variant_sources_id, source_id) VALUES (?,?)");

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
