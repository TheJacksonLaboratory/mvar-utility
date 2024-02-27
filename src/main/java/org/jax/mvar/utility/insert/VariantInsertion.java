package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.model.Assembly;
import org.jax.mvar.utility.model.Variant;
import org.jax.mvar.utility.parser.AnnotationParser;
import org.jax.mvar.utility.parser.InfoParser;
import org.jax.mvar.utility.parser.VcfParser;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class VariantInsertion {

    private final Connection connection;
    private Map<String, String[]> newTranscriptsMap;

    private final static List<String> VARIANT_TYPES = Arrays.asList("SNP", "DEL", "INS");

    private int batchSize;
    private InfoParser infoParser;
    private final Assembly assembly;
    private final boolean isLifted;

    /**
     *
     * @param batchSize   a batch number of 1000 is advised if enough memory (7G) is allocated to the JVM
     *                      Ultimately, the batch number depends on the File size and the JVM max and min memory
     * @param assembly      Can be one of the assembly enum in Assembly
     * @param isLifted      true if data to insert has been lifted from existing data in the DB
     */
    public VariantInsertion(int batchSize, Assembly assembly, boolean isLifted) {
        this.batchSize = batchSize;
        this.assembly = assembly;
        this.isLifted = isLifted;
        // create connection
        // get Properties
        Config config = new Config();
        try {
            System.out.println("Opening JDBC connection...");
            connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void closeJDBCConnection() {
        if (connection != null) {
            try {
                System.out.println("closing JDBC connection...");
                connection.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Loads a VCF file in the database
     *
     * @param vcfFile       VCF file, can be gzipped or vcf format
     * @param headerFile    If only one file is used to input data and that file has a header, then this parameter can be
     *                      the vcfFile. If not (multiple file, and they don't have a header, then a separate header file is needed.

     * @param checkForCanon if true we check for canonical

     */
    public void loadVCF(File vcfFile, File headerFile,boolean checkForCanon) throws SQLException {
        System.out.println("Parsing VCF file and inserting parsed variants into DB, " + new Date());
        System.out.println("Batch size = " + batchSize);
        // set connection

        try {
            infoParser = new AnnotationParser(headerFile);
            // parse variants into a Map
            Map<String, Variant> variations = VcfParser.parseVcf(connection, vcfFile, headerFile, checkForCanon, isLifted);
            // Persist data
            persistData(variations);
        } catch (Exception e) {
            System.err.println("An exception was caught: " + e.getMessage() + " : " + e);
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * 1. parse the vcf -- The search for duplicates is done at the parsing stage
     * 2. Persist canonicals
     * 3. persist variants
     * - canonical
     * - strain
     * - gene
     * - jannovar data
     * - external ids
     * 5. construct search doc -- TODO: possible search docs for speed querying of data in site
     *
     * @param variations    map of variations
     */
    private void persistData(Map<String, Variant> variations) throws Exception {

        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.println(new Date() + ", Starting variant insertion");

        // insert variants parsed
        int newVariantsInserted = insertVariantsBatch(variations);
        System.out.println(new Date() + "," + newVariantsInserted + " new variants inserted in " + stopWatch);
        stopWatch.reset();
    }

    /**
     * Enable/Disable ForeignKey checks, autocommit and unique checks
     *
     * @param connection jdbc connection
     * @param isEnabled  true or false
     */
    public static void innoDBSetOptions(Connection connection, boolean isEnabled) throws SQLException {
        int val = isEnabled ? 1 : 0;
        connection.setAutoCommit(isEnabled);
        PreparedStatement foreignKeyCheckStmt = null, uniqueChecksStmt = null;
        try {
            foreignKeyCheckStmt = connection.prepareStatement("SET FOREIGN_KEY_CHECKS = ?");
            foreignKeyCheckStmt.setInt(1, val);
            uniqueChecksStmt = connection.prepareStatement("SET UNIQUE_CHECKS = ?");
            uniqueChecksStmt.setInt(1, val);
            uniqueChecksStmt.execute();
            foreignKeyCheckStmt.execute();
            if (!isEnabled) connection.commit();
        } finally {
            if (foreignKeyCheckStmt != null)
                foreignKeyCheckStmt.close();
            if (uniqueChecksStmt != null)
                uniqueChecksStmt.close();
        }
    }

    private MutableObjectIntMap<?> selectAllFromColumnInList(String tableName, String columnName, Set<String> valueSet) throws SQLException {
        String listOfValueAsStr = "";
        for (String value : valueSet) {
            listOfValueAsStr = listOfValueAsStr.isEmpty() ? "'" + value + "'" : listOfValueAsStr.concat(",'").concat(value).concat("'");
        }
        String SELECT_ALL_FROM_TABLE_IN_LIST = "SELECT ID, " + columnName + " FROM " + tableName + " WHERE " + columnName + " IN (" + listOfValueAsStr + ");";
        Statement selectAllStmt = null;
        ResultSet result = null;
        MutableObjectIntMap resultMap = new ObjectIntHashMap<>();
        try {
            selectAllStmt = connection.createStatement();
            result = selectAllStmt.executeQuery(SELECT_ALL_FROM_TABLE_IN_LIST);
            while (result.next()) {
                resultMap.put(result.getString(columnName), result.getInt("ID"));
            }
        } finally {
            if (result != null)
                result.close();
            if (selectAllStmt != null)
                selectAllStmt.close();
        }
        return resultMap;
    }

    /**
     * Insert Variants, variants relationship (transcripts, strain) in batch
     *
     * @param variations LinkedHashMap of variations
     * @return number of new variants inserted
     */
    private int insertVariantsBatch(Map<String, Variant> variations) throws Exception {
        List<Variant> batchOfVars = new FastList<>();
        Set<String> geneSet = new HashSet<>();
        Set<String> transcriptSet = new HashSet<>();

        List<Map<String, String>> annotationParsed;
//        InfoParser infoParser = new AnnotationParser();
        // Retrieve the last id of canons
        int canonIdx, variantInsertedNumber;
        String selectLastIdCanonical = "select id from variant_canon_identifier order by id desc limit 1 offset 0;";
        try (PreparedStatement selectLastCanonicalIdStmt = connection.prepareStatement(selectLastIdCanonical)) {
            ResultSet idResult = selectLastCanonicalIdStmt.executeQuery();
            if (!idResult.next()) {
                canonIdx = 1;
            } else {
                canonIdx = idResult.getInt("id") + 1;
            }
            variantInsertedNumber = canonIdx;
        } catch (SQLException e) {
            throw e;
        }

        innoDBSetOptions(connection, false);

        // iterate through all the variations
        int idx = 0;
        for (String key : variations.keySet()) {
            Variant var = variations.get(key);

            batchOfVars.add(var);

            // get jannovar info
            annotationParsed = infoParser.parse(var.getJannovarAnnotation());
            for(Map<String, String> annotation : annotationParsed) {
                geneSet.add(annotation.get("Gene_Name"));
                transcriptSet.add(annotation.get("Feature_ID").split("\\.")[0]);
            }

            if (idx > 1 && idx % batchSize == 0) {
                canonIdx = batchInsertVariantsJDBC2(batchOfVars, geneSet, transcriptSet, canonIdx);
                //clear batch lists
                batchOfVars.clear();
                geneSet.clear();
                transcriptSet.clear();
            }
            idx++;
        }

        //last batch
        if (!batchOfVars.isEmpty()) {
            canonIdx = batchInsertVariantsJDBC2(batchOfVars, geneSet, transcriptSet, canonIdx);
            batchOfVars.clear();
            geneSet.clear();
            transcriptSet.clear();
        }

        // update canonical id automatically if not (mm39 && check-canon)
        if (!isLifted) {
            String UPDATE_CANONICAL_ID = "update variant_canon_identifier set caid = concat('MCA_', id) where caid is NULL";
            try (PreparedStatement updateCanonicalStmt = connection.prepareStatement(UPDATE_CANONICAL_ID)) {
                updateCanonicalStmt.execute();
                connection.commit();
            }
        }
        innoDBSetOptions(connection, true);

        // calculate the number of new variants inserted
        variantInsertedNumber = canonIdx - variantInsertedNumber;
        return variantInsertedNumber;
    }

    /**
     * Insert variants, and relationships using JDBC
     *
     * @param batchOfVars        batch of variants
     * @param geneSet            all genes for the corresponding variants
     * @param transcriptSet      all transcripts for the corresponding variants
     */
    private int batchInsertVariantsJDBC2(List<Variant> batchOfVars, Set<String> geneSet, Set<String> transcriptSet, int canonIdx) throws Exception {
        // set autocommit on for the selects stmt
        connection.setAutoCommit(true);

        // records of all unique gene symbols
        MutableObjectIntMap<?> geneSymbolRecs = selectAllFromColumnInList("gene", "symbol", geneSet);

        MutableObjectIntMap<?> geneSynonymRecs = selectAllFromColumnInList("synonym", "name", geneSet);

        MutableObjectIntMap<?> transcriptRecs = selectAllFromColumnInList("transcript", "primary_identifier", transcriptSet);

        // set autocommit off again
        connection.setAutoCommit(false);

        List<Map<String, String>> annotationParsed, originalRefTxtParsed;
        PreparedStatement insertCanonVariants = null, insertVariants = null, insertVariantTranscriptsTemp = null, insertGenotypeTemp = null, insertReftxtmm10mm39Temp = null;

        try {
            // directly use java PreparedStatement to get ResultSet with keys
            insertCanonVariants = connection.prepareStatement("insert into variant_canon_identifier (variant_ref_txt) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            insertVariants = connection.prepareStatement("insert into variant (accession, chr, position, alt, ref, type, functional_class_code, assembly, parent_ref_ind, variant_ref_txt, variant_hgvs_notation, dna_hgvs_notation, protein_hgvs_notation, impact, canon_var_identifier_id, gene_id, protein_position, amino_acid_change) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
            insertVariantTranscriptsTemp = connection.prepareStatement("insert into variant_transcript_temp (variant_ref_txt, transcript_ids, transcript_feature_ids) VALUES (?,?,?)");
            insertGenotypeTemp = connection.prepareStatement("insert into genotype_temp (variant_id, format, genotype_data) VALUES (?,?,?)");
            if (isLifted) {
                insertReftxtmm10mm39Temp = connection.prepareStatement("insert into mm10mm39temp (ref_txt_mm39, ref_txt_mm10) VALUES (?,?)");
            }

            for (Variant variant : batchOfVars) {
                // check if the variant exists
                if (!variant.getExists()) {

                    // insert into canonical table
                    insertCanonVariants.setString(1, variant.getVariantRefTxt());
                    insertCanonVariants.addBatch();

                    // get jannovar info
                    annotationParsed = infoParser.parse(variant.getJannovarAnnotation());
                    String transcriptExistingConcatIds = "", transcriptFeatureConcatIds = "";
                    for (Map<String, String> annotation : annotationParsed) {
                        int idx = annotation.get("Feature_ID").indexOf('.');
                        String transcriptId;
                        if (idx != -1)
                            transcriptId = annotation.get("Feature_ID").substring(0, idx);
                        else
                            transcriptId = annotation.get("Feature_ID");
                        transcriptExistingConcatIds = transcriptExistingConcatIds.isEmpty() ? String.valueOf(transcriptRecs.get(transcriptId)) : transcriptExistingConcatIds.concat(",").concat(String.valueOf(transcriptRecs.get(transcriptId)));
                        transcriptFeatureConcatIds = transcriptFeatureConcatIds.isEmpty() ? transcriptId : transcriptFeatureConcatIds.concat(",").concat(transcriptId);
                    }
                    // insert into temp table transcript variants
                    insertVariantTranscriptsTemp.setString(1, variant.getVariantRefTxt());
                    insertVariantTranscriptsTemp.setString(2, transcriptExistingConcatIds);
                    insertVariantTranscriptsTemp.setString(3, transcriptFeatureConcatIds);
                    insertVariantTranscriptsTemp.addBatch();

                    // insert into temp mm10/mm39 table to later properly canonicalize the new mm39 variants to their original mm10 variant if they already exist
                    if (isLifted) {
                        assert insertReftxtmm10mm39Temp != null;
                        insertReftxtmm10mm39Temp.setString(1, variant.getVariantRefTxt());
                        insertReftxtmm10mm39Temp.setString(2, variant.getOriginalRefTxt());
                        insertReftxtmm10mm39Temp.addBatch();
                    }

                    // Do we want that? to link only the most pathogenic gene info to this variant? or do we have a one to many relationship?
                    String geneName = annotationParsed.get(0).get("Gene_Name");
                    long geneId = geneSymbolRecs.get(geneName);

                    // we get the first gene info in the jannovar info string
                    if (geneId == -1) {
                        // We check in the list of synonyms to get the corresponding gene
                        geneId = getGeneBySynonyms(geneSynonymRecs, geneName);
                    }

                    insertVariants.setString(1, variant.getId());
                    insertVariants.setString(2, variant.getChr());
                    insertVariants.setInt(3, Integer.parseInt(variant.getPos()));
                    insertVariants.setString(4, variant.getAlt());
                    insertVariants.setString(5, variant.getRef());
                    insertVariants.setString(6, variant.getType());
                    String concatenations = concatenate(annotationParsed, "Annotation");
                    if (concatenations == null)
                        insertVariants.setNull(7, Types.VARCHAR);
                    else
                        insertVariants.setString(7, concatenations);
                    insertVariants.setString(8, this.assembly.label);
                    insertVariants.setBoolean(9, true);
                    // for now we put the variantRefTxt in ParentVarRef too as we are inserting variants with assembly 38 already (no liftover)
                    insertVariants.setString(10, variant.getVariantRefTxt());
                    insertVariants.setString(11, variant.getHgvsg());
                    concatenations = concatenate(annotationParsed, "HGVS.c");
                    if (concatenations == null)
                        insertVariants.setNull(12, Types.VARCHAR);
                    else
                        insertVariants.setString(12, concatenations);
                    concatenations = concatenate(annotationParsed, "HGVS.p");
                    if (concatenations == null)
                        insertVariants.setNull(13, Types.VARCHAR);
                    else
                        insertVariants.setString(13, concatenations);
                    concatenations = concatenate(annotationParsed, "Annotation_Impact");
                    if (concatenations == null)
                        insertVariants.setNull(14, Types.VARCHAR);
                    else
                        insertVariants.setString(14, concatenations);
                    insertVariants.setLong(15, canonIdx);
                    if (geneId == -1)
                        insertVariants.setNull(16, Types.BIGINT);
                    else
                        insertVariants.setLong(16, geneId);
                    insertVariants.setString(17, variant.getProteinPosition());
                    insertVariants.setString(18, variant.getAminoAcidChange());
                    insertVariants.addBatch();
                    // insert variant id to genotype temp with the current idx
                    insertGenotypeTemp.setInt(1, canonIdx);

                    canonIdx++;
                } else {
                    // insert existing variant id
                    insertGenotypeTemp.setInt(1, variant.getExistingId());
                }
                // insert into temp genotype table
                insertGenotypeTemp.setString(2, variant.getFormat());
                insertGenotypeTemp.setString(3, variant.getGenotypeData());
                insertGenotypeTemp.addBatch();

            }
            insertCanonVariants.executeBatch();
            insertVariantTranscriptsTemp.executeBatch();
            insertVariants.executeBatch();
            insertGenotypeTemp.executeBatch();
            if (isLifted) {
                assert insertReftxtmm10mm39Temp != null;
                insertReftxtmm10mm39Temp.executeBatch();
            }
            connection.commit();
            return canonIdx;
        } finally {
            if (insertCanonVariants != null)
                insertCanonVariants.close();
            if (insertVariants != null)
                insertVariants.close();
            if (insertVariantTranscriptsTemp != null)
                insertVariantTranscriptsTemp.close();
            if (insertGenotypeTemp != null)
                insertGenotypeTemp.close();
            if (insertReftxtmm10mm39Temp != null && isLifted)
                insertReftxtmm10mm39Temp.close();
        }
    }

    private String concatenate(List<Map<String, String>> annotations, String annotationKey) {
        String concatenationResult = "";
        for (Map<String, String> annot : annotations) {
            if (!concatenationResult.isEmpty()) {
                concatenationResult = concatenationResult.concat(",").concat(annot.get(annotationKey));
            } else {
                concatenationResult = annot.get(annotationKey);
            }
        }
        return concatenationResult;
    }

    /**
     * @param geneSynonymRecs   synonym of genes records
     * @param geneName          gene  name
     * @return Returns -1 if no result was found
     * @throws SQLException Exception thrown
     */
    private int getGeneBySynonyms(MutableObjectIntMap geneSynonymRecs, final String geneName) throws SQLException {
        connection.setAutoCommit(true);
        int synId = geneSynonymRecs.get(geneName);
        String selectGeneBySynId = "SELECT * FROM gene_synonym WHERE synonym_id=" + synId;
        try (Statement selectStmt = connection.createStatement(); ResultSet result = selectStmt.executeQuery(selectGeneBySynId)) {
            connection.setAutoCommit(false);
            while (result.next()) return result.getInt("gene_synonyms_id");
        }
        return -1;
    }

    private void saveNewTranscriptsToFile(String strainName) {
        String currentPath = (new File(".")).getAbsolutePath();
        File file = new File(currentPath + "/" + strainName + "_NewTranscripts.txt");
        FileWriter fr = null;
        BufferedWriter br = null;
        try {
            fr = new FileWriter(file);
            br = new BufferedWriter(fr);
            br.write("transcript_id\tgene_name\tvariant_ref_txt\tis_most_pathogenic" + System.lineSeparator());
            for (Map.Entry<String, String[]> entry : newTranscriptsMap.entrySet()) {
                br.write(entry.getKey() + "\t" + entry.getValue()[0] + "\t"
                        + entry.getValue()[1] + "\t" + entry.getValue()[2] + System.lineSeparator());
            }
        } catch (IOException e) {
            System.err.println("Error writing new transcripts to file: " + e.getMessage());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
                System.out.println("New transcripts written to file:" + file.getName());
            } catch (IOException e) {
                System.err.println("Error closing the file: " + e.getMessage());
            }
        }
    }

    /**
     * Search and update all MVAR ids for the lifted MM39 variants inserted and update the CAID for these lifted variants
     * @param startId       start id from where to perform the search and update
     * @param batchNumber   batch size
     */
    public void searchAndInsertCanonicalFromMM39(int startId, int batchNumber) {
        batchSize = batchNumber;
        Config config = new Config();
        int numberOfRecords = 0;
        int stopId = -1;

        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        System.out.println(new Date() + ", Starting searching and updating Canonical IDs for mm39");

        try {
            // count number of records
            numberOfRecords = InsertUtils.countFromTable(connection, "variant_canon_identifier", null, stopId);

            System.out.println("NumberOfRows = " + (numberOfRecords - startId + 1) + " to be parsed.");
            System.out.println("Batch size is " + batchSize);
            VariantInsertion.innoDBSetOptions(connection, false);

            int selectIdx = startId;
            long start, elapsedTimeMillis;
            Map<Integer, String> variantRefTxtMap;

            // iterate over records
            for (int i = startId - 1; i < numberOfRecords; i++) {
                if (i > startId && i % batchSize == 0) {
                    start = System.currentTimeMillis();
                    variantRefTxtMap = selectVariantRefTxt(connection, selectIdx, selectIdx + batchSize - 1);
                    updateVariantRefTxtFormm39InBatch(connection, variantRefTxtMap);
                    variantRefTxtMap.clear();
                    elapsedTimeMillis = System.currentTimeMillis() - start;
                    System.out.println("Progress: " + i + " of " + numberOfRecords + ", left: " + (numberOfRecords - i) + ", duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items updated: " + selectIdx + " to " + (selectIdx + batchSize - 1) + ", " + new Date());
                    selectIdx = selectIdx + batchSize;
                }
            }
            // last batch
            start = System.currentTimeMillis();
            variantRefTxtMap = selectVariantRefTxt(connection, selectIdx, numberOfRecords);
            if (!variantRefTxtMap.isEmpty()) {
                updateVariantRefTxtFormm39InBatch(connection, variantRefTxtMap);
                variantRefTxtMap.clear();
                elapsedTimeMillis = System.currentTimeMillis() - start;
                System.out.println("Progress: 100%, duration: " + (elapsedTimeMillis / (60 * 1000F)) + " min, items updated: " + selectIdx + " to " + numberOfRecords + ", " + new Date());
            }
            VariantInsertion.innoDBSetOptions(connection, true);

            System.out.println("mm39 canonical ids updated in " + stopWatch);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        System.out.println(new Date() + ", end timer: " + stopWatch);
        stopWatch.reset();
    }

    private static Map<Integer, String> selectVariantRefTxt(Connection connection, int start, int stop) throws SQLException {
        PreparedStatement selectStmt = null;
        ResultSet result = null;
        Map<Integer, String> variantRefTxtMap = new LinkedHashMap<>();
        connection.setAutoCommit(true);

        try {
            selectStmt = connection.prepareStatement("SELECT id, variant_ref_txt FROM variant_canon_identifier WHERE id BETWEEN ? AND ?");
            selectStmt.setInt(1, start);
            selectStmt.setInt(2, stop);
            result = selectStmt.executeQuery();
            while (result.next()) {
                int variantId = result.getInt("id");
                String variantRefTxt = result.getString("variant_ref_txt");
                variantRefTxtMap.put(variantId, variantRefTxt);
            }
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (result != null)
                result.close();
            if (selectStmt != null)
                selectStmt.close();
            connection.setAutoCommit(false);

        }
        return variantRefTxtMap;
    }

    private static void updateVariantRefTxtFormm39InBatch(Connection connection, Map<Integer, String> variantRefTxtMap) throws SQLException {
        List<String> mm10RefTxts = new LinkedList<>(), caids = new LinkedList<>();

        try (PreparedStatement updateCaidPstmt = connection.prepareStatement("UPDATE variant_canon_identifier SET caid=? WHERE variant_ref_txt=?")) {

            // select all CAIDs
            String selectRefTxtmm10 = "select ref_txt_mm10 from mm10mm39temp mmt where mmt.ref_txt_mm39 in (";
            StringBuilder refTxtList = new StringBuilder();
            // first loop to create "bulk select query"
            int idx = 0;
            for (Map.Entry<Integer, String> entry : variantRefTxtMap.entrySet()) {
                if (idx < variantRefTxtMap.size() - 1) {
                    refTxtList.append("\"").append(entry.getValue()).append("\",");
                } else {
                    // finish building the list
                    refTxtList.append("\"").append(entry.getValue()).append("\"");
                }
                idx++;
            }
            // this is required to ensure order
            selectRefTxtmm10 = selectRefTxtmm10 + refTxtList + ") ORDER BY FIELD(mmt.ref_txt_mm39," + refTxtList + ");";
            // run select statement
            try (PreparedStatement selectRefTxtmm10Stmt = connection.prepareStatement(selectRefTxtmm10); ResultSet mm10RefTxtResult = selectRefTxtmm10Stmt.executeQuery()) {
                while (mm10RefTxtResult.next()) {
                    mm10RefTxts.add(mm10RefTxtResult.getString("ref_txt_mm10"));
                }
            } catch (SQLException exc) {
                throw exc;
            }
            String selectCaids = "select caid from variant_canon_identifier vci where vci.variant_ref_txt in (";
            StringBuilder mm10RefTxtList = new StringBuilder();
            idx = 0;
            for (String mm10RefTxt : mm10RefTxts) {
                if (idx < mm10RefTxts.size() - 1) {
                    mm10RefTxtList.append("\"").append(mm10RefTxt).append("\",");
                } else {
                    mm10RefTxtList.append("\"").append(mm10RefTxt).append("\"");
                }
                idx++;
            }
            selectCaids = selectCaids + mm10RefTxtList + ") ORDER BY FIELD(vci.variant_ref_txt," + mm10RefTxtList + ");";
            try (PreparedStatement selectCaidStmt = connection.prepareStatement(selectCaids); ResultSet caidResult = selectCaidStmt.executeQuery()) {
                while (caidResult.next()) {
                    caids.add(caidResult.getString("caid"));
                }
            }
            // TODO replace the above two selects by an inner select? the problem is we need to ensure the order of the
            // results... that is why the sequencial two select is required here with a ORDER BY FIELD

            // run update in batch
            int it = 0;
            for (Map.Entry<Integer, String> entry : variantRefTxtMap.entrySet()) {
                String mm39RefTxt = entry.getValue();
                // update
                updateCaidPstmt.setString(1, caids.get(it));
                updateCaidPstmt.setString(2, mm39RefTxt);
                updateCaidPstmt.addBatch();
                it++;
            }
            updateCaidPstmt.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            mm10RefTxts.clear();
            caids.clear();
        }
    }

}