package org.jax.mvar.utility.insert;

import org.jax.mvar.utility.Utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class InsertUtils {

    /**
     * Insert into mvar_strain table new strains
     * @param connection
     * @param strainsMap
     * @throws SQLException
     */
    public static void insertIntoMvarStrain(Connection connection, Map<Integer, String> strainsMap) throws SQLException {
        // check if new strains are in the mvar strain table
        // Build the select query
        PreparedStatement selectFromMvarStrain = null;
        ResultSet resultFromMvarStrain = null;
        String mvarStrainSelectQuery = "select name, strain_id from mvar_strains where strain_id in (";
        StringBuilder sql = new StringBuilder();
        sql.append(mvarStrainSelectQuery);
        for (int i = 0; i < strainsMap.size(); i++) {
            sql.append("?");
            if (i + 1 < strainsMap.size()) {
                sql.append(",");
            }
        }
        sql.append(")");
        Map<Integer, String> missingStrains;
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
            // add missing strains to a map
            missingStrains = Utils.copyMap(strainsMap);

            while (resultFromMvarStrain.next()) {
                int strainId = resultFromMvarStrain.getInt("strain_id");
                String strainName = resultFromMvarStrain.getString("name");
                missingStrains.remove(strainId);
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
                strainLog = strainLog.equals("") ? name : strainLog + ", " + name;
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
}
