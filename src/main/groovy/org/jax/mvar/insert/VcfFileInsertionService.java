package org.jax.mvar.insert;

import org.apache.commons.lang3.time.StopWatch;

import gngs.Variant;
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

public class VcfFileInsertionService {

    private Map<String, String[]> newTranscriptsMap;
//    private static final String ENSEMBL_URL = "http://rest.ensembl.org/";

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
     * @param useType if true, the VCF parser will iterate through the types of variants in order to minimise the
     *                memory footprint in the heap (dividing by 3 since there are three type).
     */
    public void loadVCF(File vcfFile, int batchNumber, boolean useType) {
        batchSize = batchNumber;
        System.out.println("Batch size = " + batchSize + ", use_type = " + useType);

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
            persistData(connection, vcfFile, strain[0], useType);
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
        ResultSet result = selectStrainId.executeQuery("SELECT * FROM strain WHERE name LIKE \'" + strainName + "\'");
        result.next();
        String[] resultStrain = new String[2];
        resultStrain[0] = result.getString("name");
        resultStrain[1] = String.valueOf(result.getLong("id"));
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
     * @param useType if true we use type to parse the data
     */
    private void persistData(Connection connection, File vcfFile, String strainName, boolean useType) throws Exception {
        //persist data by chromosome -- TODO: check potential for multi-threaded process
        //TODO: add mouse chr to config

        // Map used to collect non-existing transcripts in DB
        // key : transcript id
        // value : list of 3 strings with 0 = gene id, 1 = variant id, 2 = most pathogenic
        newTranscriptsMap = new ConcurrentHashMap<>();
        VcfParser parser = new VcfParser();
        innoDBSetOptions(connection, false);
        if (useType) {
            for (String type : VARIANT_TYPES) {
                insert(connection, vcfFile, parser, strainName, type);
            }
        } else {
            insert(connection, vcfFile, parser, strainName, null);
        }

        // add new Transcripts to DB (we write to file for now...)
//        loadNewTranscripts((Map<String, List<String>>) newTranscriptsMap);

        innoDBSetOptions(connection, true);
    }

    private void insert(Connection connection, File vcfFile, VcfParser parser, String strainName, String type) throws Exception {
        for (String chr : MOUSE_CHROMOSOMES) {
            final StopWatch stopWatch = new StopWatch();
            stopWatch.start();

            List<Variant> vcfVariants = parser.parseVcf(chr, type, vcfFile);
            int numOfVariants = vcfVariants.size();
            System.out.println("CHR = " + chr + ", variant size= " + numOfVariants);

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

    /**
     * Enable/Disable ForeignKey checks, autocommit and unique checks
     *
     * @param connection jdbc connection
     * @param isEnabled true or false
     */
    private void innoDBSetOptions(Connection connection, boolean isEnabled) throws SQLException {
        int val = isEnabled ? 1 : 0;
        connection.setAutoCommit(isEnabled);
        PreparedStatement foreignKeyCheckStmt = connection.prepareStatement("SET FOREIGN_KEY_CHECKS = ?");
        foreignKeyCheckStmt.setInt(1, val);
        PreparedStatement uniqueChecksStmt = connection.prepareStatement("SET UNIQUE_CHECKS = ?");
        uniqueChecksStmt.setInt(1, val);
        uniqueChecksStmt.execute();
        foreignKeyCheckStmt.execute();
        if (!isEnabled) connection.commit();
    }

    /**
     * Insert Canonicals in batch
     *
     * @param connection jdbc connection
     * @param varList parsed vcf
     * @return numberOfExistingRecords
     */
    private int insertCanonVariantsBatch(Connection connection, List<Variant> varList) throws SQLException {
        String UPDATE_CANONICAL_ID = "update variant_canon_identifier set caid = concat(\'MCA_\', lpad(id, 14, 0)) where caid is NULL";

        List<Variant> batchOfVars = new ArrayList<>();
        List<String> batchOfParentVariantRef = new ArrayList<>();
        // used to search quickly if values has already been inserted and exists in the batch
        Set<String> batchOfUniqueVar = new HashSet<>();

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
        PreparedStatement updateCanonicalStmt = connection.prepareStatement(UPDATE_CANONICAL_ID);
        boolean result = updateCanonicalStmt.execute();
        connection.commit();
        return numberOfExistingRecords;
    }

    private Map<String, Long> selectAllFromColumnInList(Connection connection, String tableName, String columnName, List<String> listOfValues) throws SQLException {
        String listOfValueAsStr = "";
        for (String value : listOfValues) {
            listOfValueAsStr = listOfValueAsStr.equals("") ? "'" + value + "'" : listOfValueAsStr.concat(",'").concat(value).concat("'");
        }
        String SELECT_ALL_FROM_TABLE_IN_LIST = "SELECT ID, " + columnName + " FROM " + tableName + " WHERE " + columnName + " IN (" + listOfValueAsStr + ");";
        Statement selectAllStmt = connection.createStatement();
        ResultSet result = selectAllStmt.executeQuery(SELECT_ALL_FROM_TABLE_IN_LIST);
        Map<String, Long> resultMap = new HashMap<>();
        while (result.next()) {
            resultMap.put(result.getString(columnName), result.getLong("ID"));
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
        Map<String, Long> found = selectAllFromColumnInList(connection,"variant_canon_identifier", "variant_ref_txt", batchOfParentVariantRef);
        // set autocommit off
        connection.setAutoCommit(false);
        // insert canon variant
        PreparedStatement insertCanonVariants = connection.prepareStatement(VARIANT_CANON_INSERT, Statement.RETURN_GENERATED_KEYS);

        int numberOfExistingRecordIds = 0;
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

        insertCanonVariants.executeBatch();
        connection.commit();
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
        List<Variant> batchOfVars = new ArrayList<>();
        List<String> batchOfVariantRefTxt = new ArrayList<>();
        List<String> batchOfGenes = new ArrayList<>();
        List<String> batchOfTranscripts = new ArrayList<>();

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
        Map<String, Long> cannonRecs = selectAllFromColumnInList(connection, "variant_canon_identifier", "variant_ref_txt", batchOfVariantRefTxt);

        // records of all unique gene symbols
//        List<Gene> geneSymbolRecs = ((Class<Gene>) org.jax.mvarcore.Gene).findAllBySymbolInList(batchOfGenes);
        Map<String, Long> geneSymbolRecs = selectAllFromColumnInList(connection, "gene", "symbol", batchOfGenes);

//        List<Synonym> geneSynonymRecs = ((Class<Synonym>) org.jax.mvarcore.Synonym).findAllByNameInList(batchOfGenes)
        Map<String, Long> geneSynonymRecs = selectAllFromColumnInList(connection, "synonym", "name", batchOfGenes);

//        def transcriptsRecs = Transcript.findAllByPrimaryIdentifierInList(batchOfTranscripts)
        Map<String, Long> transcriptRecs = selectAllFromColumnInList(connection, "transcript", "primary_identifier", batchOfTranscripts);

        // set autocommit off again
        connection.setAutoCommit(false);

        // directly use java PreparedStatement to get ResultSet with keys
        PreparedStatement insertVariants = connection.prepareStatement(VARIANT_INSERT, Statement.RETURN_GENERATED_KEYS);
        PreparedStatement insertVariantTranscriptsTemp = connection.prepareStatement(VARIANT_TRANSCRIPT_TEMP);

        List<Map<String, String>> annotationParsed;
        Long canonIdentifierId;

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
            Long geneId = geneSymbolRecs.get(geneName);

            // we get the first gene info in the jannovar info string
            if (geneId == null) {
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
            if (geneId == null)
                insertVariants.setNull(14, Types.BIGINT);
            else
                insertVariants.setLong(14, Long.valueOf(geneId));
            insertVariants.setString(15, strainName);
            insertVariants.addBatch();

        }
        insertVariantTranscriptsTemp.executeBatch();
        insertVariants.executeBatch();
        connection.commit();

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

    private Long getGeneBySynonyms(Connection connection, Map<String, Long> geneSynonymRecs, final String geneName) throws SQLException {
        connection.setAutoCommit(true);
        Long synId = geneSynonymRecs.get(geneName);
        String selectGeneBySynId = "SELECT * FROM gene_synonym WHERE synonym_id=" + synId;
        Statement selectStmt = connection.createStatement();
        ResultSet result = selectStmt.executeQuery(selectGeneBySynId);
        connection.setAutoCommit(false);
        while (result.next()) {
            return result.getLong("gene_synonyms_id");
        }
        return null;
    }

//    /**
//     * Load new transcripts from the Ensembl Rest API given an ID and a List of Gene and Variant ID
//     * The Gene ID and Variant ID associated with the transcript
//     *
//     * @param ids
//     */
//    public void loadNewTranscripts(Map<String, List<String>> ids) {
//        System.out.println("*** ADD NEW TRANSCRIPTS **");
//        String lookupQuery = "lookup/id/";
//        String url = ENSEMBL_URL;
//        RestBuilder rest = new RestBuilder();
//        List<Object> transcriptList = new ArrayList();
//
//        for (Map.Entry<String, List<String>> entry : ids.entrySet()) {
//            TranscriptContainer transcript = loadNewTranscript(rest, url + lookupQuery, entry.getKey(), entry.getValue());
//            if (transcript != null)
//                transcriptList.add(transcript);
//        }
//
//        // as size might be small we set the batch size to the list size (so we have only one batch
//        saveNewTranscripts(transcriptList, transcriptList.size());
//    }
//
//    /**
//     * Load new transcript in DB given the transcript id. Using a RestBuilder, we connect to the
//     * Ensembl RESTAPI to retrieve the info if it exists.
//     *
//     * @param rest
//     * @param url
//     * @param id
//     * @param idsAndMostPathogenic
//     * @return
//     */
//    private TranscriptContainer loadNewTranscript(RestBuilder rest, String url, String id, final List<String> idsAndMostPathogenic) {
//        String fullQuery = url + id + "?content-type=application/json;expand=1";
//       RestResponse resp = rest.get(fullQuery);
//        System.out.println("Request response = " + resp.getStatusCode().value());
//        final Reference<JSONObject> jsonResult;
//        String respString = (String) resp.getBody();
//
//        if (resp.getStatusCode().value() == 200 && respString != null) {
//            int begin = respString.indexOf("{");
//            int end = respString.lastIndexOf("}") + 1;
//            respString = respString.substring(begin, end);
//            jsonResult.set(new JSONObject(respString));
//        } else {
//            getProperty("log").invokeMethod("error", new Object[]{"Response to mouse mine data request: " + resp.getStatusCode().value() + " restResponse.text= " + resp.getText()});
//            return null;
//        }
//
//        int start = DefaultGroovyMethods.asType(jsonResult.get().get("start"), (Class<Object>) Integer.class);
//        int end = DefaultGroovyMethods.asType(jsonResult.get().get("end"), (Class<Object>) Integer.class);
//        // TODO find how to get base pair length
////        int length = (end - start) + 1
//        // add variant/transcript relationship
//        Variant variant = ((Class<Variant>) org.jax.mvarcore.Variant).findById(Long.parseLong(idsAndMostPathogenic.get(1)));
//        // add gene/transcript relationship
//        Gene gene;
//        if (!idsAndMostPathogenic.get(0).equals("")) {
//            gene = ((Gene) (Gene.createCriteria().get(new Closure<Criteria>(this, this) {
//                public Criteria doCall(Object it) {
//                    return eq("mgiId", idsAndMostPathogenic.get(0));
//                }
//
//                public Criteria doCall() {
//                    return doCall(null);
//                }
//
//            })));
//        } else {
//            gene = ((Gene) (Gene.createCriteria().get(new Closure<Criteria>(this, this) {
//                public Criteria doCall(Object it) {
//                    return eq("ensemblGeneId", jsonResult.get().get("Parent"));
//                }
//
//                public Criteria doCall() {
//                    return doCall(null);
//                }
//
//            })));
//        }
//
//        TranscriptContainer container = new TranscriptContainer();
//
//
//        TranscriptContainer transcript = container.setPrimaryIdentifier(id);
//        container.setChromosome(jsonResult.get().get("seq_region_name")) container.setLocationStart(start);
//        container.setLocationEnd(end) container.setEnsGeneIdentifier(jsonResult.get().get("Parent"));
//        container.setVariant(variant) container.setGene(gene);
//        container.setMostPathogenic(Boolean.valueOf(idsAndMostPathogenic.get(2)));
//        return transcript;
//    }
//
//    private void saveNewTranscripts(List<TranscriptContainer> listOfTranscripts, final int batchSize) throws SQLException {
//        final List<TranscriptContainer> batchOfTranscripts = new ArrayList<TranscriptContainer>();
//
//        DefaultGroovyMethods.eachWithIndex(listOfTranscripts, new Closure<Object>(this, this) {
//            public Object doCall(Object transcript, Object idx) {
//                batchOfTranscripts.add((TranscriptContainer) transcript);
//                if (idx > 1 && idx % batchSize == 0) {
//                    batchInsertNewTranscriptsJDBC(batchOfTranscripts);
//                    //clear batch lists
//                    batchOfTranscripts.clear();
//                    return cleanUpGorm();
//                }
//
//            }
//
//        });
//        //last batch
//        if (listOfTranscripts.size() > 0) {
//            batchInsertNewTranscriptsJDBC(batchOfTranscripts);
//            batchOfTranscripts.clear();
//            cleanUpGorm();
//        }
//
//    }
//
//    private Integer[] batchInsertNewTranscriptsJDBC(List<TranscriptContainer> batchOfTranscripts) throws SQLException {
//        PreparedStatement insertTranscripts = connection.prepareStatement(TRANSCRIPT_INSERT, Statement.RETURN_GENERATED_KEYS);
//
//        for (TranscriptContainer transcript : batchOfTranscripts) {
//            insertTranscripts.setString(1, transcript.getPrimaryIdentifier());
//            insertTranscripts.setInt(2, transcript.getLength());
//            insertTranscripts.setString(3, transcript.getChromosome());
//            insertTranscripts.setLong(4, transcript.getLocationStart());
//            insertTranscripts.setLong(5, transcript.getLocationEnd());
//            if (transcript.getMgiGeneIdentifier() != null)
//                insertTranscripts.setString(6, transcript.getMgiGeneIdentifier());
//            else insertTranscripts.setNull(6, Types.VARCHAR);
//            insertTranscripts.setString(7, transcript.getEnsGeneIdentifier());
//            insertTranscripts.addBatch();
//        }
//
//        insertTranscripts.executeBatch();
//        ResultSet transcriptsKeys = insertTranscripts.getGeneratedKeys();
//
//        PreparedStatement insertVariantsByTranscript = connection.prepareStatement(VARIANT_TRANSCRIPT_INSERT);
//
//        Long transcriptKey;
//        for (TranscriptContainer transcript : batchOfTranscripts) {
//            // add transcripts/variant relationship
//            transcriptsKeys.next();
//            transcriptKey = transcriptsKeys.getLong(1);
//
//            // we add the transcript / variant relationship
//            insertVariantsByTranscript.setLong(1, transcript.getVariant().getId());
//            insertVariantsByTranscript.setLong(2, transcriptKey);
//            insertVariantsByTranscript.setBoolean(3, transcript.getMostPathogenic());
//            insertVariantsByTranscript.addBatch();
//
//        }
//
//        return insertVariantsByTranscript.executeBatch();
//
//    }

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