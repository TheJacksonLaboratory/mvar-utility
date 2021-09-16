package org.jax.mvar.utility.insert;

import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.parser.ParserUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class InsertUtilsTest {

    private Config config;

    @Before
    public void init() {
        config = new Config();
    }

    /**
     * Test the insertIntoMvarStrain method
     * For now it is ignored as there is no test DB to test the method on.
     */
    @Test
    @Ignore
    public void testInsertIntoMvarStrain() {
        try (Connection connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword())) {
            Map<Integer, String> strainMap = ParserUtils.getStrainsFromFile(connection, new File("src/test/resources/snpgrid_samples.txt"));
//            Map<Integer, String> strainMap = ParserUtils.getStrainsFromFile(connection, new File("src/test/resources/samples_v7.txt"));
            List<Map> strainMaps = InsertUtils.insertIntoMvarStrain(connection, strainMap);
            InsertUtils.insertMvarStrainImputed(connection, strainMaps.get(0), strainMaps.get(1), (byte)1);
            Assert.assertTrue(true);
        } catch (SQLException exc) {
            exc.printStackTrace();
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
}
