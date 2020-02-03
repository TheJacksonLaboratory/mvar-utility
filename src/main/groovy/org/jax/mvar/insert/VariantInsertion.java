package org.jax.mvar.insert;

import org.apache.commons.lang3.time.StopWatch;

import gngs.Variant;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import parser.AnnotationParser;
import parser.InfoParser;
import parser.VcfParser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

public class VariantInsertion {

    private Map<String, String[]> newTranscriptsMap;

    private final static List<String> MOUSE_CHROMOSOMES = Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "X", "Y", "MT");
    private final static List<String> VARIANT_TYPES = Arrays.asList("SNP", "DEL", "INS");

    private static final String VARIANT_CANON_INSERT = "insert into variant_canon_identifier (version, chr, position, ref, alt, variant_ref_txt) VALUES (0,?,?,?,?,?)";
    private static final String VARIANT_INSERT = "insert into variant (chr, position, alt, ref, type, functional_class_code, assembly, parent_ref_ind, parent_variant_ref_txt, variant_ref_txt, dna_hgvs_notation, protein_hgvs_notation, canon_var_identifier_id, gene_id, strain_name) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
    private static final String VARIANT_TRANSCRIPT_TEMP = "insert into variant_transcript_temp (variant_ref_txt, transcript_ids, transcript_feature_ids) VALUES (?,?,?)";

    private static int batchSize = 1000;
    private String assembly;
    private InfoParser infoParser = new AnnotationParser();

    /**
     * Loads a VCF file in the database
     * @param vcfFile VCF file, can be gzipped or vcf format
     * @param batchNumber a batch number of 1000 is advised if enough memory (7G) is allocated to the JVM
     *                    Ultimately, the batch number depends on the File size and the JVM max and min memory
     * @param types Can be ALL, SNP, DEL or INS. the VCF parser will iterate through the types of variants in order to minimise the
     *                memory footprint in the heap (dividing by 3 since there are three type).
     */
    public void loadVCF(File vcfFile, int batchNumber, String[] types) {
        batchSize = batchNumber;
        String typeName = null;
        for (String type : types)
            typeName = typeName == null ? type : typeName.concat(",").concat(type);
        System.out.println("Batch size = " + batchSize + ", type = " + typeName);

        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        String vcfFileName = vcfFile.getName();
        String[] strTmpArray = vcfFileName.split("\\.");
        // the assembly should be the beginning of the filename followed by '.'
        assembly = strTmpArray[0];
        // the strain name should be after the assembly name after the '.' character
        String strainName = strTmpArray[1];

        if (!isAcceptedAssembly(assembly.toLowerCase())) {
            //Invalid file name. Expecting assembly as the first part of the file name
            return;
        }
        System.out.println("Vcf File: " + vcfFileName);
        // get Properties
        Config config = new Config();
        
        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            // get strain id/name
            String[] strain = getStrainName(connection, strainName);
            // Persist data
            persistData(connection, vcfFile, strain[0], types);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("An exception was caught: " + e.getMessage());
        }
        // save new transcripts to file
        saveNewTranscriptsToFile(strainName);

        System.out.println("Vcf file complete parsing and persistence: " + stopWatch + ", " + new Date());
    }

    /**
     * We expect the strain name taken from the file name to be the same number of char as the strain
     * name in the database
     *
     * @param connection jdbc connection
     * @param strainName strain name to look for
     * @return name of strain
     */
    private String[] getStrainName(Connection connection, String strainName) throws SQLException {
        Statement selectStrainId = connection.createStatement();
        ResultSet result = null;
        String[] resultStrain;
        try {
            result = selectStrainId.executeQuery("SELECT * FROM strain WHERE name LIKE \'" + strainName + "\'");
            result.next();
            resultStrain = new String[2];
            resultStrain[0] = result.getString("name");
            resultStrain[1] = String.valueOf(result.getLong("id"));
            result.close();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (result != null)
                result.close();
        }
        return resultStrain;
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
     * @param strainName name of the strain
     * @param types ALL, SNP, DEL and/or INS
     */
    private void persistData(Connection connection, File vcfFile, String strainName, String[] types) throws Exception {
        //persist data by chromosome -- TODO: check potential for multi-threaded process
        //TODO: add mouse chr to config

        // Map used to collect non-existing transcripts in DB
        // key : transcript id
        // value : list of 3 strings with 0 = gene id, 1 = variant id, 2 = most pathogenic
        if (newTranscriptsMap == null)
            newTranscriptsMap = new ConcurrentHashMap<>();
        else
            newTranscriptsMap.clear();
        VcfParser parser = new VcfParser();
        innoDBSetOptions(connection, false);

        insert(connection, vcfFile, parser, strainName, types);
        // add new Transcripts to DB (we write to file for now...)
//        loadNewTranscripts((Map<String, List<String>>) newTranscriptsMap);

        innoDBSetOptions(connection, true);
    }

    private void insert(Connection connection, File vcfFile, VcfParser parser, String strainName, String[] types) throws Exception {
        for (String type : types) {
            for (String chr : MOUSE_CHROMOSOMES) {
                final StopWatch stopWatch = new StopWatch();
                stopWatch.start();

                List<Variant> vcfVariants = parser.parseVcf(chr, type, vcfFile);
                int numOfVariants = vcfVariants.size();
                System.out.println("CHR = " + chr + ", variant size= " + numOfVariants + ", type=" + type);

                //insert canonicals
                int numOfExistingRecords = insertCanonVariantsBatch(connection, vcfVariants);
                //insert variants, transcript, hgvs and relationships, and collect new transcripts not in DB
                insertVariantsBatch(connection, vcfVariants, strainName);
                System.out.println("CHR,SIZE,DURATION,DATE");
                System.out.println(chr + "," + numOfVariants + "," + stopWatch + "," + new Date());
                System.out.println("Number of existing records in canonicals: " + numOfExistingRecords);
                stopWatch.reset();
                stopWatch.start();
            }
        }
    }

    /**
     * Enable/Disable ForeignKey checks, autocommit and unique checks
     *
     * @param connection jdbc connection
     * @param isEnabled true or false
     */
    private void innoDBSetOptions(Connection connection, boolean isEnabled) throws SQLException {
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
        } catch(SQLException exc) {
            throw exc;
        } finally {
            if (foreignKeyCheckStmt != null)
                foreignKeyCheckStmt.close();
            if (uniqueChecksStmt != null)
                uniqueChecksStmt.close();
        }
    }

    /**
     * Insert Canonicals in batch
     *
     * @param connection jdbc connection
     * @param varList parsed vcf
     * @return numberOfExistingRecords
     */
    private int insertCanonVariantsBatch(Connection connection, List<Variant> varList) throws SQLException {

        List<Variant> batchOfVars = new FastList<>();
        List<String> batchOfParentVariantRef = new FastList<>();
        // used to search quickly if values has already been inserted and exists in the batch
        Set<String> batchOfUniqueVar = new UnifiedSet<>();

        int idx = 0, numberOfExistingRecords = 0;
        for (Variant var : varList) {
            String position = var.getInfo().get("OriginalStart") != null ? (String) var.getInfo().get("OriginalStart") : String.valueOf(var.getPos());
            String chromosome = var.getChr().replace("ch", "").replace("r", "");
            String parentVariantRef = chromosome.concat("_").concat(position).concat("_").concat(var.getRef()).concat("_").concat(var.getAlt());

            // we add the variant to the batch only if it not already there
            // to avoid having duplicates inside one batch which hasn't been yet committed to the DB
            if (!batchOfUniqueVar.contains(parentVariantRef)) {
                batchOfVars.add(var);
                batchOfParentVariantRef.add(parentVariantRef);
            }
            batchOfUniqueVar.add(parentVariantRef);

            if (idx > 1 && idx % batchSize == 0) {
                numberOfExistingRecords = numberOfExistingRecords + batchInsertCannonVariantsJDBC(connection, batchOfVars, batchOfParentVariantRef);
                //clear batch lists
                batchOfVars.clear();
                batchOfParentVariantRef.clear();
                batchOfUniqueVar.clear();
            }
            idx++;
        }

        //last batch
        if (batchOfVars.size() > 0) {
            numberOfExistingRecords = numberOfExistingRecords + batchInsertCannonVariantsJDBC(connection, batchOfVars, batchOfParentVariantRef);
            batchOfVars.clear();
            batchOfParentVariantRef.clear();
            batchOfUniqueVar.clear();
        }

        // update canonical id
        String UPDATE_CANONICAL_ID = "update variant_canon_identifier set caid = concat(\'MCA_\', lpad(id, 14, 0)) where caid is NULL";
        PreparedStatement updateCanonicalStmt = null;
        try {
            updateCanonicalStmt = connection.prepareStatement(UPDATE_CANONICAL_ID);
            updateCanonicalStmt.execute();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (updateCanonicalStmt != null)
                updateCanonicalStmt.close();
        }
        return numberOfExistingRecords;
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
        } catch(SQLException exc) {
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
     * Insert Canonicals using JDBC
     *
     * @param connection jdbc connection
     * @param batchOfVars
     * @param batchOfParentVariantRef
     * @return numberOfExistingRecordIds
     */
    private int batchInsertCannonVariantsJDBC(Connection connection, List<Variant> batchOfVars, List<String> batchOfParentVariantRef) throws SQLException {
        // set autocommit on
        connection.setAutoCommit(true);
        MutableObjectLongMap found = selectAllFromColumnInList(connection,"variant_canon_identifier", "variant_ref_txt", batchOfParentVariantRef);
        // set autocommit off
        connection.setAutoCommit(false);
        // insert canon variant
        PreparedStatement insertCanonVariants = null;
        int numberOfExistingRecordIds = 0;
        try {
            insertCanonVariants = connection.prepareStatement(VARIANT_CANON_INSERT, Statement.RETURN_GENERATED_KEYS);
            for (Variant variant : batchOfVars) {
                String position = variant.getInfo().get("OriginalStart") != null ? (String) variant.getInfo().get("OriginalStart") : String.valueOf(variant.getPos());
                String chromosome = variant.getChr().replace("ch", "").replace("r", "");
                String parentRefVariant = chromosome.concat("_").concat(position).concat("_").concat(variant.getRef()).concat("_").concat(variant.getAlt());

                if (found.containsKey(parentRefVariant)) {
                    numberOfExistingRecordIds++;
                } else {
                    // chromosome
                    insertCanonVariants.setString(1, chromosome);
                    insertCanonVariants.setInt(2, Integer.parseInt(position));
                    insertCanonVariants.setString(3, variant.getRef());
                    insertCanonVariants.setString(4, variant.getAlt());
                    insertCanonVariants.setString(5, parentRefVariant);
                    insertCanonVariants.addBatch();
                }
            }
            // If numberOfExisting records is equal to ths batch size : no need to execute batch if it is empty
            if (numberOfExistingRecordIds != batchOfVars.size()) {
                insertCanonVariants.executeBatch();
                connection.commit();
            }
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertCanonVariants != null)
                insertCanonVariants.close();
        }
        return numberOfExistingRecordIds;
    }

    /**
     * Insert Variants, variants relationship (transcripts, strain) in batch
     *
     * @param connection jdbc connection
     * @param varList    vcf list
     * @param strainName strain name
     */
    private void insertVariantsBatch(Connection connection, List<Variant> varList, final String strainName) throws Exception {
        List<Variant> batchOfVars = new FastList<>();
        List<String> batchOfVariantRefTxt = new FastList<>();
        List<String> batchOfGenes = new FastList<>();
        List<String> batchOfTranscripts = new FastList<>();

        List<Map<String, String>> annotationParsed;
        InfoParser infoParser = new AnnotationParser();
        int idx = 0;
        for (Variant var : varList) {
            batchOfVars.add(var);

            // Retrieve values
            // TODO Take into account the lift over from mm9 to mm10 when inserting original mm9 data
            String position = var.getInfo().get("OriginalStart") != null ? (String) var.getInfo().get("OriginalStart") : String.valueOf(var.getPos());
            String chromosome = var.getChr().replace("ch", "").replace("r", "");
            String variantRefTxt = chromosome.concat("_").concat(position).concat("_").concat(var.getRef()).concat("_").concat(var.getAlt());

//            batchOfParentVariantRefTxt.add(parentRefVariant);
//            String variantRefTxt = parentRefVariant;
            batchOfVariantRefTxt.add(variantRefTxt);

            // get jannovar info
            annotationParsed = infoParser.parse("ANN=".concat((String) var.getInfo().get("ANN")));
            batchOfGenes.add(annotationParsed.get(0).get("Gene_Name"));
            batchOfTranscripts.add(annotationParsed.get(0).get("Feature_ID").split("\\.")[0]);

            if (idx > 1 && idx % batchSize == 0) {
                batchInsertVariantsJDBC(connection, batchOfVars, batchOfVariantRefTxt, batchOfGenes, batchOfTranscripts, strainName);
                //clear batch lists
                batchOfVars.clear();
                batchOfVariantRefTxt.clear();
                batchOfGenes.clear();
                batchOfTranscripts.clear();
            }
            idx++;
        }

        //last batch
        if (batchOfVars.size() > 0) {
            batchInsertVariantsJDBC(connection, batchOfVars, batchOfVariantRefTxt, batchOfGenes, batchOfTranscripts, strainName);
            batchOfVars.clear();
            batchOfVariantRefTxt.clear();
            batchOfGenes.clear();
            batchOfTranscripts.clear();
        }

    }

    /**
     * Insert variants, and relationships using JDBC
     *
     * @param connection jdbc connection
     * @param batchOfVars
     * @param batchOfVariantRefTxt
     * @param batchOfGenes
     * @param batchOfTranscripts
     * @param strainName
     */
    private void batchInsertVariantsJDBC(Connection connection, List<Variant> batchOfVars, List<String> batchOfVariantRefTxt, List<String> batchOfGenes, List<String> batchOfTranscripts, String strainName) throws Exception {
        // set autocommit on for the selects stmt
        connection.setAutoCommit(true);
//        Map<String, Long> found = selectAllFromColumnInList("variant", "variant_ref_txt", batchOfVariantRefTxt);

        // records of all unique canon ids
//        List<VariantCanonIdentifier> cannonRecs = ((Class<VariantCanonIdentifier>) org.jax.mvarcore.VariantCanonIdentifier).findAllByVariantRefTxtInList(batchOfParentVariantRefTxt);
        MutableObjectLongMap cannonRecs = selectAllFromColumnInList(connection, "variant_canon_identifier", "variant_ref_txt", batchOfVariantRefTxt);

        // records of all unique gene symbols
//        List<Gene> geneSymbolRecs = ((Class<Gene>) org.jax.mvarcore.Gene).findAllBySymbolInList(batchOfGenes);
        MutableObjectLongMap geneSymbolRecs = selectAllFromColumnInList(connection, "gene", "symbol", batchOfGenes);

//        List<Synonym> geneSynonymRecs = ((Class<Synonym>) org.jax.mvarcore.Synonym).findAllByNameInList(batchOfGenes)
        MutableObjectLongMap geneSynonymRecs = selectAllFromColumnInList(connection, "synonym", "name", batchOfGenes);

//        def transcriptsRecs = Transcript.findAllByPrimaryIdentifierInList(batchOfTranscripts)
        MutableObjectLongMap transcriptRecs = selectAllFromColumnInList(connection, "transcript", "primary_identifier", batchOfTranscripts);

        // set autocommit off again
        connection.setAutoCommit(false);

        List<Map<String, String>> annotationParsed;
        long canonIdentifierId;
        PreparedStatement insertVariants = null, insertVariantTranscriptsTemp = null;

        try {
            // directly use java PreparedStatement to get ResultSet with keys
            insertVariants = connection.prepareStatement(VARIANT_INSERT);
            insertVariantTranscriptsTemp = connection.prepareStatement(VARIANT_TRANSCRIPT_TEMP);

            for (Variant variant : batchOfVars) {
                // retrieve values
                String position = variant.getInfo().get("OriginalStart") != null ? (String) variant.getInfo().get("OriginalStart") : String.valueOf(variant.getPos());
                String chromosome = variant.getChr().replace("ch", "").replace("r", "");
                String variantRefTxt = chromosome.concat("_").concat(position).concat("_").concat(variant.getRef()).concat("_").concat(variant.getAlt());

                canonIdentifierId = cannonRecs.get(variantRefTxt);

                // get jannovar info
                annotationParsed = infoParser.parse("ANN=".concat((String) variant.getInfo().get("ANN")));
                String transcriptExistingConcatIds = "", transcriptFeatureConcatIds = "";
                for (int i = 0; i < annotationParsed.size(); i++) {
                    String transcriptId = annotationParsed.get(i).get("Feature_ID").split("\\.")[0];
                    // check if transcript already exists, if not we add it to the map of new transcripts
                    if (!transcriptRecs.containsKey(transcriptId) && !newTranscriptsMap.containsKey(transcriptId)) {
                        // gene name, variant ref txt, most pathogenic
                        if (!newTranscriptsMap.containsKey(transcriptId))
                            newTranscriptsMap.put(transcriptId, new String[]{annotationParsed.get(i).get("Gene_Name"), variantRefTxt, String.valueOf(i == 0)});
                    }
                    transcriptExistingConcatIds = transcriptExistingConcatIds.equals("") ? String.valueOf(transcriptRecs.get(transcriptId)) : transcriptExistingConcatIds.concat(",").concat(String.valueOf(transcriptRecs.get(transcriptId)));
                    transcriptFeatureConcatIds = transcriptFeatureConcatIds.equals("") ? transcriptId : transcriptFeatureConcatIds.concat(",").concat(transcriptId);
                }
                // insert into temp table transcript variants
                insertVariantTranscriptsTemp.setString(1, variantRefTxt);
                insertVariantTranscriptsTemp.setString(2, transcriptExistingConcatIds);
                insertVariantTranscriptsTemp.setString(3, transcriptFeatureConcatIds);
                insertVariantTranscriptsTemp.addBatch();

                // Do we want that? to link only the most pathogenic gene info to this variant? or do we have a one to many relationship?
                String geneName = annotationParsed.get(0).get("Gene_Name");
                long geneId = geneSymbolRecs.get(geneName);

                // we get the first gene info in the jannovar info string
                if (geneId == -1) {
                    // We check in the list of synonyms to get the corresponding gene
                    geneId = getGeneBySynonyms(connection, geneSynonymRecs, geneName);
                }

                insertVariants.setString(1, chromosome);
                insertVariants.setInt(2, Integer.parseInt(position));
                insertVariants.setString(3, variant.getAlt());
                insertVariants.setString(4, variant.getRef());
                insertVariants.setString(5, variant.getType());
                String concatenations = concatenate(annotationParsed, "Annotation");
                if (concatenations == null)
                    insertVariants.setNull(6, Types.VARCHAR);
                else
                    insertVariants.setString(6, concatenations);
                insertVariants.setString(7, assembly);
                insertVariants.setBoolean(8, isRefAssembly(assembly));
                // for now we put the variantRefTxt in ParentVarRef too as we are inserting variants with assembly 38 already (no liftover)
                insertVariants.setString(9, variantRefTxt);
                insertVariants.setString(10, variantRefTxt);
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
                insertVariants.setLong(13, canonIdentifierId);
                if (geneId == -1)
                    insertVariants.setNull(14, Types.BIGINT);
                else
                    insertVariants.setLong(14, geneId);
                insertVariants.setString(15, strainName);
                insertVariants.addBatch();

            }
            insertVariantTranscriptsTemp.executeBatch();
            insertVariants.executeBatch();
            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertVariants != null)
                insertVariants.close();
            if (insertVariantTranscriptsTemp != null)
                insertVariantTranscriptsTemp.close();
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
     *
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
        ResultSet result =  null;
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
        try{
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
        }finally{
            try {
                br.close();
                fr.close();
                System.out.println("New transcripts written to file:" + file.getName());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private boolean isAcceptedAssembly(String inAssembly) {
        // TODO define configuration for accepted assemblies
        List<String> assemblies = new ArrayList<>(Arrays.asList("grcm38", "ncbi37", "ncbi36"));
        return assemblies.contains(inAssembly);
    }

    private boolean isRefAssembly(String inAssembly) {
        // TODO define configuration for reference assembly
        String refAssembly = "grcm38";
        return inAssembly.equals(refAssembly);
    }

}