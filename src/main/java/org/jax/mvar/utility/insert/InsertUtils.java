package org.jax.mvar.utility.insert;

import org.jax.mvar.utility.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class InsertUtils {

    /**
     * Insert into mvar_strain table new strains
     * @param connection
     * @param strainsMap
     * @return a linked list of two Maps, the first is the existing strains,
     *  and the second is the missing strains
     * @throws SQLException
     */
    public static List<Map> insertIntoMvarStrain(Connection connection, Map<Integer, String> strainsMap) throws SQLException {
        // result lists maps (the 1rst one is the existingStrains, the second one is the missingStrains
        List<Map> resultStrainMaps = new LinkedList<>();
        // check if new strains are in the mvar strain table
        // Build the select query
        PreparedStatement selectFromMvarStrain = null;
        ResultSet resultFromMvarStrain = null;
        String mvarStrainSelectQuery = "select name, strain_id from mvar_strain where strain_id in (";
        StringBuilder sql = new StringBuilder();
        sql.append(mvarStrainSelectQuery);
        for (int i = 0; i < strainsMap.size(); i++) {
            sql.append("?");
            if (i + 1 < strainsMap.size()) {
                sql.append(",");
            }
        }
        sql.append(")");
        // add missing strains to a map
        Map<Integer, String> missingStrains = Utils.copyMap(strainsMap);
        // already existing strains
        Map<Integer, String> existingStrains = new LinkedHashMap<>();
        try {
            selectFromMvarStrain = connection.prepareStatement(sql.toString());

            int idx = 0;
            for (Map.Entry<Integer, String> entry : strainsMap.entrySet()) {
                int strainId = entry.getKey();
                String name = entry.getValue();
                selectFromMvarStrain.setInt(idx + 1, strainId);
                idx++;
            }
            resultFromMvarStrain = selectFromMvarStrain.executeQuery();

            int count = 0;
            while (resultFromMvarStrain.next()) {
                int strainId = resultFromMvarStrain.getInt("strain_id");
                String strainName = resultFromMvarStrain.getString("name");
                missingStrains.remove(strainId);
                existingStrains.put(strainId, strainName);
                count++;
            }
            System.out.println(count + " strains are already in the mvar_strain table.");
        } catch (SQLException exc) {
            exc.printStackTrace();
        } finally {
            if (resultFromMvarStrain != null)
                resultFromMvarStrain.close();
            if (selectFromMvarStrain != null)
                selectFromMvarStrain.close();
        }
        resultStrainMaps.add(existingStrains);
        if (missingStrains.size() != 0) {
            // insert missing strains into mvar strain table
            PreparedStatement insertStrainsStmt = connection.prepareStatement("INSERT INTO mvar_strain (name, strain_id)  VALUES (?, ?)");

            try {
                String strainLog = "";
                for (Map.Entry<Integer, String> strain : missingStrains.entrySet()) {
                    int id = strain.getKey();
                    String name = strain.getValue();
                    // insert id into strain table (MVAR strains)
                    insertStrainsStmt.setString(1, name);
                    insertStrainsStmt.setInt(2, id);
                    insertStrainsStmt.addBatch();
                    strainLog = strainLog.equals("") ? name : strainLog + ", " + name;
                }
                insertStrainsStmt.executeBatch();
                System.out.println("Strains " + strainLog + " added to mvar_strain table.");
            } catch (SQLException exc) {
                throw exc;
            } finally {
                if (insertStrainsStmt != null)
                    insertStrainsStmt.close();
            }
        }
        resultStrainMaps.add(missingStrains);
        return resultStrainMaps;
    }

    /**
     * Insert mvar Strain-imputed relationships.
     * @param connection
     * @param missingStrains
     * @param existingStrains
     * @param imputed
     * @throws SQLException
     */
    public static void insertMvarStrainImputed(Connection connection, Map<Integer, String> existingStrains, Map<Integer, String> missingStrains, byte imputed) throws SQLException {

        // retrieve imputed id
        int imputedId = -1;
        PreparedStatement selectFromImputedStmt = null;
        ResultSet  resultImputedId = null;
        try {
            // get the imputed id
            selectFromImputedStmt = connection.prepareStatement("select id from imputed where imputed=?");
            selectFromImputedStmt.setByte(1, imputed);
            resultImputedId = selectFromImputedStmt.executeQuery();
            resultImputedId.next();
            imputedId =  resultImputedId.getInt("id");
        } finally {
            if (resultImputedId != null)
                resultImputedId.close();
            if (selectFromImputedStmt != null)
                selectFromImputedStmt.close();
        }

        // insert existing strain imputed relationship
        PreparedStatement insertMvarStrainsImputedStmt = null;

        try {
            // we add the existing mvar strain imputed relationship
            insertMvarStrainsImputedStmt = connection.prepareStatement("INSERT INTO mvar_strain_imputed (mvar_strain_imputeds_id, imputed_id)  VALUES (?, ?)");

            // check existing strain imputed relationship
            for (Map.Entry<Integer, String> existingStrain : existingStrains.entrySet()) {
                PreparedStatement selectFromMvarStrainImputedStmt = null;
                ResultSet mvarStrainExistingImputedResult = null;

                try {
                    // get the corresponding mvar_strain id
                    int mvarStrainId = getMvarStrainId(connection, existingStrain.getValue());

                    selectFromMvarStrainImputedStmt = connection.prepareStatement("select * from mvar_strain_imputed where mvar_strain_imputeds_id=? and imputed_id=?");
                    selectFromMvarStrainImputedStmt.setInt(1, mvarStrainId);
                    selectFromMvarStrainImputedStmt.setInt(2, imputedId);
                    mvarStrainExistingImputedResult = selectFromMvarStrainImputedStmt.executeQuery();
                    if (!mvarStrainExistingImputedResult.next()) {
                        // we add the existing mvar strain imputed relationship
                        insertMvarStrainsImputedStmt.setInt(1, mvarStrainId);
                        insertMvarStrainsImputedStmt.setInt(2, imputedId);
                        insertMvarStrainsImputedStmt.addBatch();
                    }
                } finally {
                    if (mvarStrainExistingImputedResult != null)
                        mvarStrainExistingImputedResult.close();
                    if (selectFromMvarStrainImputedStmt != null)
                        selectFromMvarStrainImputedStmt.close();
                }
            }

            // execute the insertion of existing strains
            insertMvarStrainsImputedStmt.executeBatch();

            // insert missing strains imputed relationship
            insertMvarStrainsImputedStmt = connection.prepareStatement("INSERT INTO mvar_strain_imputed (mvar_strain_imputeds_id, imputed_id)  VALUES (?, ?)");

            for (Map.Entry<Integer, String> strain : missingStrains.entrySet()) {

                int mvarStrainId = getMvarStrainId(connection, strain.getValue());
                insertMvarStrainsImputedStmt.setInt(1, mvarStrainId);
                insertMvarStrainsImputedStmt.setInt(2, imputedId);
                insertMvarStrainsImputedStmt.addBatch();
            }
            insertMvarStrainsImputedStmt.executeBatch();

        } finally {
            if (insertMvarStrainsImputedStmt != null)
                insertMvarStrainsImputedStmt.close();
        }
    }

    private static int getMvarStrainId(Connection connection, String strainName) throws SQLException {
        PreparedStatement selectIdfromMvarStrainsStmt = null;
        ResultSet mvarStrainIdResult = null;
        int mvarStrainId;
        try {
            // get the corresponding mvar_strain id
            selectIdfromMvarStrainsStmt = connection.prepareStatement("select id from mvar_strain where name=?");
            selectIdfromMvarStrainsStmt.setString(1, strainName);
            mvarStrainIdResult = selectIdfromMvarStrainsStmt.executeQuery();
            mvarStrainIdResult.next();
            mvarStrainId = mvarStrainIdResult.getInt("id");
        } finally {
            if (mvarStrainIdResult != null)
                mvarStrainIdResult.close();
            if (selectIdfromMvarStrainsStmt != null)
                selectIdfromMvarStrainsStmt.close();
        }
        return mvarStrainId;
    }
}
