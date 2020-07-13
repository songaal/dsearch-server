package com.danawa.fastcatx.server.services;

import com.danawa.fastcatx.server.entity.JdbcRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;


@Service
public class JdbcService {
    private static Logger logger = LoggerFactory.getLogger(RankingTuningService.class);

    public JdbcService() { }

    public boolean connectionTest(JdbcRequest jdbcRequest){
        boolean flag = false;
        try{
            /* MySQL */
//            Driver : com.mysql.jdbc.Driver
//            URL   : jdbc:mysql://localhost:3306/DBNAME
            /* Altibase */
//            - Driver Name : Altibase
//            - Class Name : Altibase.jdbc.driver.AltibaseDriver
//            - URL Template : jdbc:Altibase://{host}[:{port}]/{database}
//            - Default Port : 20300
            /* Oracle */
//            oracle.jdbc.driver.OracleDriver
//            jdbc:oracle:thin:@{host}:{port}/{database}

            String url = jdbcRequest.getUrl() + jdbcRequest.getAddress() + ":" + jdbcRequest.getPort() + "/" + jdbcRequest.getDB_name();
            Class.forName(jdbcRequest.getDriver());
            Connection connection = null;
            connection = DriverManager.getConnection(url, jdbcRequest.getUser(), jdbcRequest.getPassword());
            connection.close();
            flag = true;
        }catch (SQLException sqlException){
            System.out.println(sqlException);
        }catch (ClassNotFoundException classNotFoundException){
            System.out.println(classNotFoundException);
        } catch (Exception e){
            System.out.println(e);
        }
        return flag;
    }
}
