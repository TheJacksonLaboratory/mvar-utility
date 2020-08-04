package org.jax.mvar.utility.insert;

import org.apache.commons.lang3.time.StopWatch;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.model.Variant;
import org.jax.mvar.utility.parser.AnnotationParser;
import org.jax.mvar.utility.parser.InfoParser;
import org.jax.mvar.utility.parser.VcfParser;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class VariantInsertion {

    private Map<String, String[]> newTranscriptsMap;

    private final static List<String> MOUSE_CHROMOSOMES = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "X", "Y", "MT");
    private final static List<String> VARIANT_TYPES = Arrays.asList("SNP", "DEL", "INS");
    private final static String[] STRAIN_LIST = { "129P2/OlaHsd", "129S1/SvImJ", "129S5/SvEvBrd", "AKR/J", "A/J",  "BALB/cJ", "BTBR T<+> Itpr3<tf>/J", "BUB/BnJ", "C3H/HeH", "C3H/HeJ", "C57BL/10J", "C57BL/6NJ", "C57BR/cdJ", "C57L/J", "C58/J", "CAST/EiJ", "CBA/J", "DBA/1J", "DBA/2J", "FVB/NJ", "I/LnJ", "JF1/MsJ", "KK/HlJ", "LEWES/EiJ", "LG/J", "LP/J", "MOLF/EiJ", "NOD/ShiLtJ", "NZB/B1NJ", "NZO/HlLtJ", "NZW/LacJ", "PL/J", "PWK/PhJ", "QSi3", "QSi5", "RF/J", "SEA/GnJ", "SJL/J", "SM/J", "SPRET/EiJ", "ST_bJ", "WSB/EiJ", "ZALENDE/EiJ" };

    private static final String VARIANT_CANON_INSERT = "insert into variant_canon_identifier (version, chr, position, ref, alt, variant_ref_txt) VALUES (0,?,?,?,?,?)";
    private static final String VARIANT_INSERT = "insert into variant (chr, position, alt, ref, type, functional_class_code, assembly, parent_ref_ind, parent_variant_ref_txt, variant_ref_txt, dna_hgvs_notation, protein_hgvs_notation, canon_var_identifier_id, gene_id, strain_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
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
        LinkedHashMap<String, Variant> variations = VcfParser.parseVcf(vcfFile, STRAIN_LIST);
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
                // retrieve values TODO
                String strainName = "";

                // insert into canonical table
                insertCanonVariants.setString(1, variant.getChr());
                insertCanonVariants.setInt(2, Integer.parseInt(variant.getPos()));
                insertCanonVariants.setString(3, variant.getRef());
                insertCanonVariants.setString(4, variant.getAlt());
                insertCanonVariants.setString(5, variant.getVariantRefTxt());
                insertCanonVariants.addBatch();

                // get jannovar info
                annotationParsed = infoParser.parse(variant.getAnnotation());
                String transcriptExistingConcatIds = "", transcriptFeatureConcatIds = "";
                for (Map<String, String> annotation : annotationParsed) {
                    String transcriptId = annotation.get("Feature_ID").split("\\.")[0];
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

                insertVariants.setString(1, variant.getChr());
                insertVariants.setInt(2, Integer.parseInt(variant.getPos()));
                insertVariants.setString(3, variant.getAlt());
                insertVariants.setString(4, variant.getRef());
                insertVariants.setString(5, variant.getType());
                String concatenations = concatenate(annotationParsed, "Annotation");
                if (concatenations == null)
                    insertVariants.setNull(6, Types.VARCHAR);
                else
                    insertVariants.setString(6, concatenations);
                insertVariants.setString(7, ASSEMBLY);
                insertVariants.setBoolean(8, true);
//                insertVariants.setBoolean(8, isRefAssembly(assembly));
                // for now we put the variantRefTxt in ParentVarRef too as we are inserting variants with assembly 38 already (no liftover)
                insertVariants.setString(9, variant.getVariantRefTxt());
                insertVariants.setString(10, variant.getVariantRefTxt());
                concatenations = concatenate(annotationParsed, "HGVS.c");
                if (concatenations == null)
                    insertVariants.setNull(11, Types.VARCHAR);
                else
                    insertVariants.setString(11, concatenations);
                concatenations = concatenate(annotationParsed, "HGVS.p");
                if (concatenations == null)
                    insertVariants.setNull(12, Types.VARCHAR);
                else
                    insertVariants.setString(12, concatenations);
                insertVariants.setLong(13, canonIdx);
                if (geneId == -1)
                    insertVariants.setNull(14, Types.BIGINT);
                else
                    insertVariants.setLong(14, geneId);
                insertVariants.setString(15, strainName);
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
        String concatenationResult;
        for (Map<String, String> annot : annotations) {
            concatenationResult = "";
            for (Object key : annot.keySet()) {
                if (key.equals(annotationKey)) {
                    if (!concatenationResult.equals("")) {
                        concatenationResult = concatenationResult.concat(",").concat(annot.get(annotationKey));
                    } else {
                        concatenationResult = annot.get(annotationKey);
                    }
                }
            }
            return concatenationResult;
        }
        return null;
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