package org.jax.mvar.utility.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class ParserUtils {

    /**
     * Retrieve all the corresponding Sample ids given a a sample file.
     * If the samples in the sample file are not present in the mvar_strain table, the corresponding
     * samples will be added to that table.
     *
     * @param connection sql connection
     * @param strainFile sample file
     * @return List of all strain ids
     * @throws Exception
     */
    public static List<Integer> getStrainIds(Connection connection, File strainFile) throws Exception {
        // Collect list of strains from strainFile
        // get the file names from a list a the chromosome file names is not in the wanted order
        final List<String> strains = new ArrayList<>();
        try {
            try (BufferedReader in = new BufferedReader(new FileReader(strainFile))) {
                String line = in.readLine(); // read a line at a time
                while (line != null) { // loop till you have no more lines
                    strains.add(line); // add the line to your list
                    line = in.readLine(); // try to read another line
                }
                System.out.println(strains.size() + " samples were found in the sample file.");
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }

        // collect the list of all the strain ids given the list of strain names for the Sanger data
        PreparedStatement strainIdStmt = null;
        ResultSet resultStrainIds = null;
        List<Integer> strainIds = new ArrayList<>();
        List<String> strainNames = new ArrayList<>();
        List<String> unfound = new ArrayList<>();
        String strStrainIds = "";
        for (int i = 0; i < strains.size(); i++) {
            try {
                strainIdStmt = connection.prepareStatement("SELECT id, name from strain where name like ?");
                strainIdStmt.setString(1, strains.get(i) + "%");
                resultStrainIds = strainIdStmt.executeQuery();
                if (resultStrainIds.next()) {
                    int id = resultStrainIds.getInt("id");
                    String name = resultStrainIds.getString("name");
                    strainIds.add(id);
                    strainNames.add(name);
                    strStrainIds = strStrainIds + ":" + id;
                } else {
                    // not found
                    unfound.add(strains.get(i));
                }
            } catch (SQLException exc) {
                throw exc;
            } finally {
                if (resultStrainIds != null)
                    resultStrainIds.close();
                if (strainIdStmt != null)
                    strainIdStmt.close();
            }
        }
        if (unfound.size() == 0) {
            System.out.println("All samples were found in the DB.");
        } else {
            System.out.println("The following samples were not found in the DB:");
            for (String strain : unfound) {
                System.out.println(strain);
            }
            // We stop the insertion as if there is a missing strain, the relationship insertion will fail.
            throw new IllegalStateException("Error finding the above strains. Make sure that all the strains in the strain file " +
                    "provided are present in the strain table.");
        }
        // INSERT in MVAR Strain table
        insertIntoMvarStrain(connection, strainNames);

        System.out.println("The list of strain Ids is the following : " + strStrainIds);
        return strainIds;
    }

    private static void insertIntoMvarStrain(Connection connection, List<String> strainNames) throws SQLException {
        // check if new strains are in the mvar strain table
        // Build the select query
        PreparedStatement selectFromMvarStrain = null;
        ResultSet resultFromMvarStrain = null;
        LinkedHashMap<Integer, String> missingStrains = new LinkedHashMap<>();
        String mvarStrainSelectQuery = "select name, strain_id from mvar_strains where name in (";
        StringBuilder sql = new StringBuilder();
        sql.append(mvarStrainSelectQuery);
        for (int i = 0; i < strainNames.size(); i++) {
            sql.append("?");
            if (i + 1 < strainNames.size()) {
                sql.append(",");
            }
        }
        sql.append(")");

        try {
            selectFromMvarStrain = connection.prepareStatement(sql.toString());
            int idx = 0;
            for (String strain : strainNames) {
                selectFromMvarStrain.setString(idx + 1, strain);
                idx++;
            }
            resultFromMvarStrain = selectFromMvarStrain.executeQuery();
            // add missing strains to a map
            while (resultFromMvarStrain.next()) {
                int strainId = resultFromMvarStrain.getInt("strain_id");
                String strainName = resultFromMvarStrain.getString("name");
                missingStrains.put(strainId, strainName);
            }
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (resultFromMvarStrain != null)
                resultFromMvarStrain.close();
            if (selectFromMvarStrain != null)
                selectFromMvarStrain.close();
        }

        // insert missing strains into mvar strain table
        PreparedStatement insertStrainsStmt = connection.prepareStatement("INSERT INTO mvar_strains (name, strain_id)  VALUES (?, ?)");
        try {
            String strainLog = "";
            for (Map.Entry<Integer, String> strain : missingStrains.entrySet()) {
                int id = strain.getKey();
                String name = strain.getValue();
                // insert id into strain table (MVAR strains)
                insertStrainsStmt.setString(1, name);
                insertStrainsStmt.setInt(2, id);
                insertStrainsStmt.addBatch();
                if (strainLog.equals("")) {
                    strainLog = name;
                } else {
                    strainLog = strainLog + ", " + name;
                }
            }
            insertStrainsStmt.executeBatch();
            System.out.println("Strains " + strainLog + " added to mvar_strain table.");
//            connection.commit();
        } catch (SQLException exc) {
            throw exc;
        } finally {
            if (insertStrainsStmt != null)
                insertStrainsStmt.close();
        }
    }

    /**
     * Returns the header of the vcf file as a String
     *
     * @param vcfFile
     * @return
     */
    public static String getHeader(File vcfFile) {
        String header = "";
        try (BufferedReader in = new BufferedReader(new FileReader(vcfFile))) {
            String line = in.readLine(); // read a line at a time
            int idx = 0;
            while (line != null && line.startsWith("##")) { // loop till you have no more lines
                header = header + line + "\n"; // add the line to your list
                line = in.readLine(); // try to read another line
                idx++;
            }
            System.out.println("There are " +  idx + " header lines in the vcf file.");
        } catch (Exception exc) {
            exc.printStackTrace();
        }
        return header;
    }

    /**
     * Method that reads the VCF header and given an annotation id (ANN, CSQ, etc)
     * returns the found annotations keys that make up that id.
     *
     * @param id
     * @param header
     * @return
     */
    public static List<String> getAnnotationKeys(String id, String header) throws Exception {
        List<String> annotations = new ArrayList<>();
        String[] headerLines = header.split("\n");
        if (headerLines.length <= 1)
            throw new Exception("The header string provided does not have enough lines.");
        for (String line : headerLines) {
            if (line.startsWith("##INFO=<ID=" + id)) {
                String[] descriptionAnnotation = line.split("\"");
                if (!id.equals("ANN") && !id.equals("CSQ") && !id.equals("SVANN")) {
                    annotations.add(id);
                    return annotations;
                }
                String annotation = descriptionAnnotation[1].split(":")[1];
                annotation = annotation.trim();
                annotation = annotation.replaceAll("\\'", "");
                String[] keys = annotation.split("\\|");
                annotations = Arrays.asList(keys);
                return annotations;
            }
        }
        throw new Exception(("There is no header line for the \"" + id + "\" id."));
    }
}
