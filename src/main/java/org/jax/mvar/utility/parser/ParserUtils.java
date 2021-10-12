package org.jax.mvar.utility.parser;

import org.jax.mvar.utility.Utils;

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
     * @return Map of strains (where key/value is strain_id/name)
     * @throws Exception
     */
    public static Map<Integer, String> getStrainsFromFile(Connection connection, File strainFile) throws Exception {
        // Collect list of strains from strainFile
        // get the file names from a list a the chromosome file names is not in the wanted order
        final List<String> strains = new ArrayList<>();
        // important to keep order
        final Map<Integer, String> strainMap = new LinkedHashMap<>();
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
        if (strains.size() == 0) throw new AssertionError("The strain file could not be properly read.");

        // collect the list of all the strain ids given the list of strain names for the Sanger data
        PreparedStatement strainIdStmt = null;
        ResultSet resultStrainIds = null;
        List<String> unfound = new ArrayList<>();
        String strStrainIds = "";
        for (int i = 0; i < strains.size(); i++) {
            try {
                strainIdStmt = connection.prepareStatement("SELECT id, name from strain where name=?");
                strainIdStmt.setString(1, strains.get(i));
                resultStrainIds = strainIdStmt.executeQuery();
                if (resultStrainIds.next()) {
                    int id = resultStrainIds.getInt("id");
                    String name = resultStrainIds.getString("name");
                    strainMap.put(id, name);
                    strStrainIds = strStrainIds.equals("") ? String.valueOf(id) : strStrainIds + ":" + id;
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
            // search in synonyms
            for (String strain : unfound) {
                try {
                    strainIdStmt = connection.prepareStatement("SELECT id, name, synonyms from strain where synonyms like ?");
                    strainIdStmt.setString(1, "%" + strain + "%");
                    resultStrainIds = strainIdStmt.executeQuery();
                    while (resultStrainIds.next()) {
                        int id = resultStrainIds.getInt("id");
                        String name = resultStrainIds.getString("name");
                        String synonyms = resultStrainIds.getString("synonyms");
                        String[] synonymsArray = synonyms.split("\\|");
                        for (String synonym : synonymsArray) {
                            if (synonym.equals(strain)) {
                                System.out.println("Strain id " + id + " with name " + name + " and synonym " + synonym +
                                        " was found in the DB. Please make sure the strain name is the same in the strain file as the one in the DB.");
                            }
                        }
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
            // We stop the insertion as if there is a missing strain, the relationship insertion will fail.
            throw new IllegalStateException("Error finding the above strains. Make sure that all the strains in the strain file " +
                    "provided are present in the strain table and that the names are identical.");
        }
        System.out.println("The list of strain Ids is the following : " + strStrainIds);
        return strainMap;
    }

    /**
     * Returns the header of the vcf file as a String
     *
     * @param headerFile can be a header file or a vcf file with a header
     * @return
     */
    public static String getHeader(File headerFile) {
        String header = "";
        try (BufferedReader in = new BufferedReader(new FileReader(headerFile))) {
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
     * @return list of annotation keys
     * @throws Exception
     */
    public static List<String> getAnnotationKeys(String id, String header) throws Exception {
        List<String> annotations = new LinkedList<>();
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
