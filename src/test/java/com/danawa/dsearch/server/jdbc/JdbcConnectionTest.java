package com.danawa.dsearch.server.jdbc;

import com.danawa.dsearch.server.jdbc.dto.JdbcUpdateRequest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;


public class JdbcConnectionTest {
    @Test
    @DisplayName("마리아DB 연결 테스트")
    public void mariaDB_Connection_Test() {
        // 도커로 간단히 테스트
        // docker run -d -p 3306:3306 --name mariadb --env MARIADB_DATABASE=test --env MARIADB_USER=danawa --env MARIADB_PASSWORD=ekskdhk --env MARIADB_ROOT_PASSWORD=ekskdhk1!  mariadb:latest
        try{
            /* Maria DB */
            JdbcUpdateRequest jdbcRequest = new JdbcUpdateRequest();
            jdbcRequest.setUrl("jdbc:mariadb://");
            jdbcRequest.setAddress("localhost");
            jdbcRequest.setPort("3306");
            jdbcRequest.setDB_name("test");
            jdbcRequest.setUser("danawa");
            jdbcRequest.setPassword("ekskdhk");
            jdbcRequest.setDriver("org.mariadb.jdbc.Driver");

            Properties prop = new Properties();

            prop.put("user", jdbcRequest.getUser());
            prop.put("password", jdbcRequest.getPassword());

            String url = jdbcRequest.getUrl() + jdbcRequest.getAddress() + ":" + jdbcRequest.getPort() + "/" + jdbcRequest.getDB_name();
            Class.forName(jdbcRequest.getDriver());

            try(Connection connection = DriverManager.getConnection(url, prop)){
                Assertions.assertFalse(connection.isClosed()); // 커넥션이 살아 있는지 확인
            }
        }catch (SQLException sqlException){
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
    }
    @Test
    @DisplayName("Mysql 연결 테스트")
    public void mysql_Connection_Test() {
        try{
            /* MySql */
            JdbcUpdateRequest jdbcRequest = new JdbcUpdateRequest();
            jdbcRequest.setUrl("jdbc:mysql://");
            jdbcRequest.setAddress("dev.danawa.com");
            jdbcRequest.setPort("3306");
            jdbcRequest.setDB_name("dbBoard");
            jdbcRequest.setUser("DEdevelop1_R");
            jdbcRequest.setPassword("vpflehxm#*^^");
            /* Deprecated */
            jdbcRequest.setDriver("com.mysql.jdbc.Driver");
            /* Newer */
            jdbcRequest.setDriver("com.mysql.cj.jdbc.Driver");

            Properties prop = new Properties();
            prop.put("serverTimezone", "Asia/Seoul");
            prop.put("user", jdbcRequest.getUser());
            prop.put("password", jdbcRequest.getPassword());

            String url1 = jdbcRequest.getUrl() + jdbcRequest.getAddress() + ":" + jdbcRequest.getPort() + "/" + jdbcRequest.getDB_name();
            Class.forName(jdbcRequest.getDriver());

            try(Connection connection = DriverManager.getConnection(url1, prop)){
                System.out.println("connection 성공");
            }

        }catch (SQLException sqlException){
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
    }
    @Test
    @DisplayName("Altibase 연결 테스트")
    public void altibase_Connection_Test() {
        try{
            /* AltiBase */
            JdbcUpdateRequest jdbcRequest = new JdbcUpdateRequest();
            jdbcRequest.setUrl("jdbc:Altibase://");
            jdbcRequest.setAddress("112.175.252.198");
            jdbcRequest.setPort("20200");
            jdbcRequest.setDB_name("DNWALTI");
            jdbcRequest.setUser("DELINKDATA_R"); // ?
            jdbcRequest.setPassword("tpdlwl#*^^"); // ?
            jdbcRequest.setDriver("Altibase.jdbc.driver.AltibaseDriver");
            String url2 = jdbcRequest.getUrl() + jdbcRequest.getAddress() + ":" + jdbcRequest.getPort() + "/" + jdbcRequest.getDB_name();
            Class.forName(jdbcRequest.getDriver());

            Properties prop = new Properties();
            prop.put("user", jdbcRequest.getUser());
            prop.put("password", jdbcRequest.getPassword());

            try(Connection connection = DriverManager.getConnection(url2, prop)){
                System.out.println("connection 성공");
            }

        }catch (SQLException sqlException){
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
    }
    @Test
    @DisplayName("오라클DB 연결 테스트")
    public void oracleDB_Connection_Test() {
        try{
            /* Oracle */
            JdbcUpdateRequest jdbcRequest3 = new JdbcUpdateRequest();
            jdbcRequest3.setUrl("jdbc:mysql://");
            jdbcRequest3.setAddress("127.0.0.1");
            jdbcRequest3.setPort("3306");
            jdbcRequest3.setDB_name("test");
            jdbcRequest3.setUser("root");
            jdbcRequest3.setPassword("qwe123");
            jdbcRequest3.setDriver("oracle.jdbc.driver.OracleDriver");
            String url3 = jdbcRequest3.getUrl() + jdbcRequest3.getAddress() + ":" + jdbcRequest3.getPort() + "/" + jdbcRequest3.getDB_name();
            Class.forName(jdbcRequest3.getDriver());
            Connection connection3 = null;
            connection3 = DriverManager.getConnection(url3, jdbcRequest3.getUser(), jdbcRequest3.getPassword());
            connection3.close();
        }catch (SQLException sqlException){
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
    }
}
