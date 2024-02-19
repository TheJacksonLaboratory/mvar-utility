package org.jax.mvar.utility.insert;

import org.jax.mvar.utility.Config;
import org.jax.mvar.utility.parser.ParserUtils;
import org.junit.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class InsertUtilsTest {

    private Config config;
    Connection connection;
    @Before
    public void init() {
        config = new Config();

        try {
            connection = DriverManager.getConnection(config.getUrl(), config.getUser(), config.getPassword());
            // set auto commit false so any operation in this test will be discarded.
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void teardown() {
        try {
            connection.setAutoCommit(true);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Test the insertIntoMvarStrain method
     * For now it is ignored as there is no test DB to test the method on.
     */
    @Test
    @Ignore
    public void testInsertIntoMvarStrain() {
        try {
            Map<Integer, String> strainMap = ParserUtils.getStrainsFromFile(connection, new File("src/test/resources/snpgrid_samples.txt"));
//            Map<Integer, String> strainMap = ParserUtils.getStrainsFromFile(connection, new File("src/test/resources/samples_v7.txt"));
            List<Map> strainMaps = InsertUtils.insertIntoMvarStrain(connection, strainMap);
            InsertUtils.insertMvarStrainImputed(connection, strainMaps.get(0), strainMaps.get(1), (byte)1);
            Assert.assertTrue(true);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }
}
