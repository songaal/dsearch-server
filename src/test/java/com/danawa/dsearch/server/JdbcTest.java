package com.danawa.dsearch.server;

import com.danawa.dsearch.server.jdbc.entity.JdbcRequest;
import org.junit.jupiter.api.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@SpringBootTest
public class JdbcTest {


    Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    @Test
    public void connection() {
        try{
            /* MySql */
//            JdbcRequest jdbcRequest = new JdbcRequest();
//            jdbcRequest.setUrl("jdbc:mysql://");
//            jdbcRequest.setAddress("127.0.0.1");
//            jdbcRequest.setPort("3306");
//            jdbcRequest.setDB_name("test");
//            jdbcRequest.setUser("root");
//            jdbcRequest.setPassword("qwe123");
//            /* Deprecated */
//            jdbcRequest.setDriver("com.mysql.jdbc.Driver");
//            /* Newer */
//            jdbcRequest.setDriver("com.mysql.cj.jdbc.Driver");
//            String url1 = jdbcRequest.getUrl() + jdbcRequest.getAddress() + ":" + jdbcRequest.getPort() + "/" + jdbcRequest.getDB_name();
//            Class.forName(jdbcRequest.getDriver());
//            Connection connection = null;
//            connection = DriverManager.getConnection(url1, jdbcRequest.getUser(), jdbcRequest.getPassword());
//            connection.close();


            /* AltiBase */
            JdbcRequest jdbcRequest2 = new JdbcRequest();
            jdbcRequest2.setUrl("jdbc:Altibase://");
            jdbcRequest2.setAddress("112.175.252.198");
            jdbcRequest2.setPort("20200");
            jdbcRequest2.setDB_name("DNWALTI");
            jdbcRequest2.setUser("danawa"); // ?
            jdbcRequest2.setPassword("qwe123"); // ?
            jdbcRequest2.setDriver("Altibase.jdbc.driver.AltibaseDriver");
            String url2 = jdbcRequest2.getUrl() + jdbcRequest2.getAddress() + ":" + jdbcRequest2.getPort() + "/" + jdbcRequest2.getDB_name();
            Class.forName(jdbcRequest2.getDriver());
            Connection connection2 = null;
            connection2 = DriverManager.getConnection(url2, jdbcRequest2.getUser(), jdbcRequest2.getPassword());
            connection2.close();


            /* Oracle */
//            JdbcRequest jdbcRequest3 = new JdbcRequest();
//            jdbcRequest3.setUrl("jdbc:mysql://");
//            jdbcRequest3.setAddress("127.0.0.1");
//            jdbcRequest3.setPort("3306");
//            jdbcRequest3.setDB_name("test");
//            jdbcRequest3.setUser("root");
//            jdbcRequest3.setPassword("qwe123");
//            jdbcRequest3.setDriver("oracle.jdbc.driver.OracleDriver");
//            String url3 = jdbcRequest3.getUrl() + jdbcRequest3.getAddress() + ":" + jdbcRequest3.getPort() + "/" + jdbcRequest3.getDB_name();
//            Class.forName(jdbcRequest3.getDriver());
//            Connection connection3 = null;
//            connection3 = DriverManager.getConnection(url3, jdbcRequest3.getUser(), jdbcRequest3.getPassword());
//            connection3.close();

        }catch (SQLException sqlException){
            logger.error("", sqlException);
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            logger.error("", classNotFoundException);
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
    }
}
