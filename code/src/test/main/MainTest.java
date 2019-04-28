package main;

import driver.Main;
import org.apache.commons.pool.ObjectPool;
import org.junit.Before;
import org.junit.Test;
import org.testng.Assert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class MainTest {

    final static String MYSQL_USERNAME = "root"; //local
    final static String MYSQL_PWD = "example"; //local
    final static String TABLE = "bcm"; //local/bcm
    final static String MYSQL_HOST = "192.168.64.2"; //local

    ObjectPool pool;

    @Before
    public void setup() throws IOException {
        pool = Main.initMySqlConnectionPool(MYSQL_HOST,"3306","bcm",MYSQL_USERNAME,MYSQL_PWD);
    }

    @Test
    public void DStreamTest() {

    }

    @Test
    public void databaseInsertTest() throws Exception {
        String insertOp = "INSERT INTO "+TABLE+"(region,hour, temperature,temperature3hoursbefore) VALUES ('24','2019-04-26 09',10,10)";
        Connection conn = (Connection)pool.borrowObject();
        Statement st = conn.createStatement();

        Assert.assertTrue(st.execute(insertOp));
    }

    @Test
    public void databaseUpdateTest() throws Exception {
        String updateOp = "UPDATE "+TABLE+" SET temperature3hoursbefore=20 WHERE region='24'";

        Connection conn = (Connection)pool.borrowObject();
        Statement st = conn.createStatement();

        Assert.assertTrue(st.execute(updateOp));
    }

    @Test
    public void databaseQueryTest() throws Exception {
        String queryOp = "select * from "+TABLE+" where region='24'";
        Connection conn = (Connection)pool.borrowObject();
        Statement st = conn.createStatement();
        ResultSet res = st.executeQuery(queryOp);
//        while (res.next()) {
//            String dbrec = (String.valueOf(res.getString(1))+","+res.getString(2) +","+ String.valueOf(res.getDouble(3))+","+String.valueOf(res.getDouble(4)));
//            System.out.println(dbrec);
//        }

        Assert.assertTrue(res.next());
    }
}