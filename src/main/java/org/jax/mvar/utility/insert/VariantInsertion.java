package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.model.Variant;
import org.jax.mvar.utility.parser.AnnotationParser;
import org.jax.mvar.utility.parser.ConsequenceParser;
import org.jax.mvar.utility.parser.InfoParser;
import org.jax.mvar.utility.parser.VcfParser;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class VariantInsertion {

    private Map<String, String[]> newTranscriptsMap;

    private final static List<String> VARIANT_TYPES = Arrays.asList("SNP", "DEL", "INS");

    private static final String VARIANT_CANON_INSERT = "insert into variant_canon_identifier (variant_ref_txt) VALUES (?)";
    private static final String VARIANT_INSERT = "insert into variant (accession, chr, position, alt, ref, type, functional_class_code, assembly, parent_ref_ind, variant_ref_txt, variant_hgvs_notation, dna_hgvs_notation, protein_hgvs_notation, impact, canon_var_identifier_id, gene_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String VARIANT_TRANSCRIPT_TEMP = "insert into variant_transcript_temp (variant_ref_txt, transcript_ids, transcript_feature_ids) VALUES (?,?,?)";
    private static final String GENOTYPE_TEMP = "insert into genotype_temp (format, genotype_data) VALUES (?,?)";

    private static int batchSize = 1000;
    private static final String ASSEMBLY = "grcm38";
    private InfoParser infoParser = new AnnotationParser();

    /**
     * Loads a VCF file in the database
     *
     * @param vcfFile     VCF file, can be gzipped or vcf format
     * @param batchNumber a batch number of 1000 is advised if enough memory (7G) is allocated to the JVM
     *                    Ultimately, the batch number depends on the File size and the JVM max and min memory
     */
    public void loadVCF(File vcfFile, int batchNumber) {
        batchSize = batchNumber;
        System.out.println("Batch size = " + batchSize );

        // get Properties
        Config config = new Config();
        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            // Persist data
            persistData(connection, vcfFile);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An exception was caught: " + e.getMessage());
        }
    }

    /**
     * 1. parse the vcf -- by chromosome ,
     * 2. Persist canonicals
     * 3. persist variants
     * 4. TODO persist variant/transcript associations
     * - canonical
     * - strain
     * - gene
     * - jannovar data
     * - external ids
     * 5. construct search doc -- TODO: possible search docs for speed querying of data in site
     *
     * @param connection jdbc connection
     * @param vcfFile    file
     */
    private void persistData(Connection connection, File vcfFile) throws Exception {
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // parse variants into a Map
        LinkedHashMap<String, Variant> variations = VcfParser.parseVcf(vcfFile);
        // insert variants parsed
        insertVariantsBatch(connection, variations);
        System.out.println(variations.keySet().size() + " inserted in " + stopWatch + ", " + new Date());
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
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (foreignKeyCheckStmt != null)
                foreignKeyCheckStmt.close();
            if (uniqueChecksStmt != null)
                uniqueChecksStmt.close();
        }
    }

    private MutableObjectLongMap selectAllFromColumnInList(Connection connection, String tableName, String columnName, List<String> listOfValues) throws SQLException {
        String listOfValueAsStr = "";
        for (String value : listOfValues) {
            listOfValueAsStr = listOfValueAsStr.equals("") ? "'" + value + "'" : listOfValueAsStr.concat(",'").concat(value).concat("'");
        }
        String SELECT_ALL_FROM_TABLE_IN_LIST = "SELECT ID, " + columnName + " FROM " + tableName + " WHERE " + columnName + " IN (" + listOfValueAsStr + ");";
        Statement selectAllStmt = null;
        ResultSet result = null;
        MutableObjectLongMap resultMap = new ObjectLongHashMap();
        try {
            selectAllStmt = connection.createStatement();
            result = selectAllStmt.executeQuery(SELECT_ALL_FROM_TABLE_IN_LIST);
            while (result.next()) {
                resultMap.put(result.getString(columnName), result.getLong("ID"));
            }
        } catch (SQLException exc) {
            throw exc;
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
     * @param connection jdbc connection
     * @param variations LinkedHashMap of variations
     */
    private void insertVariantsBatch(Connection connection, LinkedHashMap<String, Variant> variations) throws Exception {
        List<Variant> batchOfVars = new FastList<>();
        List<String> batchOfGenes = new FastList<>();
        List<String> batchOfTranscripts = new FastList<>();

        List<Map<String, String>> annotationParsed;
        InfoParser infoParser = new AnnotationParser();
        // Retrieve the last id of canons
        int canonIdx;
        String selectLastIdCanonical = "select id from variant_canon_identifier order by id desc limit 1 offset 0;";
        try (PreparedStatement selectLastCanonicalIdStmt = connection.prepareStatement(selectLastIdCanonical)) {
            ResultSet idResult = selectLastCanonicalIdStmt.executeQuery();
            if (!idResult.next()) {
                canonIdx = 1;
            } else {
                canonIdx = idResult.getInt("id") + 1;
            }
        }

        innoDBSetOptions(connection, false);

        // iterate through all the variations
        int idx = 0;
        for (String key : variations.keySet()) {
            Variant var = variations.get(key);

            batchOfVars.add(var);

            // get jannovar info
            annotationParsed = infoParser.parse(var.getAnnotation());
            batchOfGenes.add(annotationParsed.get(0).get("Gene_Name"));
            batchOfTranscripts.add(annotationParsed.get(0).get("Feature_ID").split("\\.")[0]);

            if (idx > 1 && idx % batchSize == 0) {
                canonIdx = batchInsertVariantsJDBC2(connection, batchOfVars, batchOfGenes, batchOfTranscripts, canonIdx);
                //clear batch lists
                batchOfVars.clear();
                batchOfGenes.clear();
                batchOfTranscripts.clear();
            }
            idx++;
        }

        //last batch
        if (batchOfVars.size() > 0) {
            batchInsertVariantsJDBC2(connection, batchOfVars, batchOfGenes, batchOfTranscripts, canonIdx);
            batchOfVars.clear();
            batchOfGenes.clear();
            batchOfTranscripts.clear();
        }

        // update canonical id
        String UPDATE_CANONICAL_ID = "update variant_canon_identifier set caid = concat(\'MCA_\', id) where caid is NULL";
        try (PreparedStatement updateCanonicalStmt = connection.prepareStatement(UPDATE_CANONICAL_ID)) {
            updateCanonicalStmt.execute();
            connection.commit();
        }

        innoDBSetOptions(connection, true);
    }

    /**
     * Insert variants, and relationships using JDBC
     *
     * @param connection           jdbc connection
     * @param batchOfVars
     * @param batchOfGenes
     * @param batchOfTranscripts
     */
    private int batchInsertVariantsJDBC2(Connection connection, List<Variant> batchOfVars, List<String> batchOfGenes, List<String> batchOfTranscripts, int canonIdx) throws Exception {
        // set autocommit on for the selects stmt
        connection.setAutoCommit(true);

        // records of all unique gene symbols
        MutableObjectLongMap geneSymbolRecs = selectAllFromColumnInList(connection, "gene", "symbol", batchOfGenes);

        MutableObjectLongMap geneSynonymRecs = selectAllFromColumnInList(connection, "synonym", "name", batchOfGenes);

        MutableObjectLongMap transcriptRecs = selectAllFromColumnInList(connection, "transcript", "primary_identifier", batchOfTranscripts);

        // set autocommit off again
        connection.setAutoCommit(false);

        List<Map<String, String>> annotationParsed;
        PreparedStatement insertCanonVariants = null, insertVariants = null, insertVariantTranscriptsTemp = null, insertGenotypeTemp = null;

        try {
            // directly use java PreparedStatement to get ResultSet with keys
            insertCanonVariants = connection.prepareStatement(VARIANT_CANON_INSERT, Statement.RETURN_GENERATED_KEYS);
            insertVariants = connection.prepareStatement(VARIANT_INSERT);
            insertVariantTranscriptsTemp = connection.prepareStatement(VARIANT_TRANSCRIPT_TEMP);
            insertGenotypeTemp = connection.prepareStatement(GENOTYPE_TEMP);

            for (Variant variant : batchOfVars) {
                // insert into canonical table
                insertCanonVariants.setString(1, variant.getVariantRefTxt());
                insertCanonVariants.addBatch();

                // get jannovar info
                annotationParsed = infoParser.parse(variant.getAnnotation());
                String transcriptExistingConcatIds = "", transcriptFeatureConcatIds = "";
                for (Map<String, String> annotation : annotationParsed) {
                    int idx = annotation.get("Feature_ID").indexOf('.');
                    String transcriptId;
                    if (idx != -1)
                        transcriptId = annotation.get("Feature_ID").substring(0, idx);
                    else
                        transcriptId = annotation.get("Feature_ID");
                    transcriptExistingConcatIds = transcriptExistingConcatIds.equals("") ? String.valueOf(transcriptRecs.get(transcriptId)) : transcriptExistingConcatIds.concat(",").concat(String.valueOf(transcriptRecs.get(transcriptId)));
                    transcriptFeatureConcatIds = transcriptFeatureConcatIds.equals("") ? transcriptId : transcriptFeatureConcatIds.concat(",").concat(transcriptId);
                }
                // insert into temp table transcript variants
                insertVariantTranscriptsTemp.setString(1, variant.getVariantRefTxt());
                insertVariantTranscriptsTemp.setString(2, transcriptExistingConcatIds);
                insertVariantTranscriptsTemp.setString(3, transcriptFeatureConcatIds);
                insertVariantTranscriptsTemp.addBatch();

                // insert into temp genotype table
                insertGenotypeTemp.setString(1, variant.getFormat());
                insertGenotypeTemp.setString(2, variant.getGenotypeData());
                insertGenotypeTemp.addBatch();

                // Do we want that? to link only the most pathogenic gene info to this variant? or do we have a one to many relationship?
                String geneName = annotationParsed.get(0).get("Gene_Name");
                long geneId = geneSymbolRecs.get(geneName);

                // we get the first gene info in the jannovar info string
                if (geneId == -1) {
                    // We check in the list of synonyms to get the corresponding gene
                    geneId = getGeneBySynonyms(connection, geneSynonymRecs, geneName);
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
                insertVariants.setString(8, ASSEMBLY);
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
                insertVariants.addBatch();

                canonIdx++;

            }
            insertCanonVariants.executeBatch();
            insertVariantTranscriptsTemp.executeBatch();
            insertVariants.executeBatch();
            insertGenotypeTemp.executeBatch();
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
        }
    }

    private String concatenate(List<Map<String, String>> annotations, String annotationKey) {
        String concatenationResult = "";
        for (Map<String, String> annot : annotations) {
            if (!concatenationResult.equals("")) {
                concatenationResult = concatenationResult.concat(",").concat(annot.get(annotationKey));
            } else {
                concatenationResult = annot.get(annotationKey);
            }
        }
        return concatenationResult;
    }

    /**
     * @param connection
     * @param geneSynonymRecs
     * @param geneName
     * @return Returns -1 if no result was found
     * @throws SQLException
     */
    private long getGeneBySynonyms(Connection connection, MutableObjectLongMap geneSynonymRecs, final String geneName) throws SQLException {
        connection.setAutoCommit(true);
        long synId = geneSynonymRecs.get(geneName);
        String selectGeneBySynId = "SELECT * FROM gene_synonym WHERE synonym_id=" + synId;
        Statement selectStmt = null;
        ResultSet result = null;
        try {
            selectStmt = connection.createStatement();
            result = selectStmt.executeQuery(selectGeneBySynId);
            connection.setAutoCommit(false);
            while (result.next()) return result.getLong("gene_synonyms_id");
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (result != null)
                result.close();
            if (selectStmt != null)
                selectStmt.close();
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
            br.write("transcript_id\tgene_name\tvariant_ref_txt\tis_most_pathogenic" + System.getProperty("line.separator"));
            for (Map.Entry<String, String[]> entry : newTranscriptsMap.entrySet()) {
                br.write(entry.getKey() + "\t" + entry.getValue()[0] + "\t"
                        + entry.getValue()[1] + "\t" + entry.getValue()[2] + System.getProperty("line.separator"));
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.err.println("Error writing new transcripts to file: " + e.getMessage());
        } finally {
            try {
                if (br != null)
                    br.close();
                if (fr != null)
                    fr.close();
                System.out.println("New transcripts written to file:" + file.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}