package com.danawa.fastcatx.server;

import com.danawa.fastcatx.server.entity.RankingTuningRequest;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcTest {
    public static void main(String[] args) {
        /* 인덱스 생성 */
//        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 80, "http"));
//        try(RestHighLevelClient client = new RestHighLevelClient(builder)) {
//            /* 생성 */
//            CreateIndexRequest createIndexRequest = new CreateIndexRequest("fastcatx_jdbc");
//            createIndexRequest.settings(Settings.builder()
//                    .put("index.number_of_shards", 1)
//                    .put("index.number_of_replicas", 0)
//            );
//
//            Map<String, Object> mapping = new HashMap<>();
//            Map<String, Object> properties = new HashMap<>();
//
//            /* properties */
//            Map<String, Object> message = new HashMap<>();
//            message.put("type", "text");
//            properties.put("ID", message);
//            properties.put("NAME", message);
//            properties.put("PROVIDER", message);
//            properties.put("DRIVER", message);
//            properties.put("ADDRESS", message);
//            properties.put("PORT", message);
//            properties.put("DB_NAME", message);
//            properties.put("USER", message);
//            properties.put("PASSWORD", message);
//            properties.put("PARAMS", message);
//            properties.put("URL", message);
//            mapping.put("properties", properties);
//            createIndexRequest.mapping(mapping);
//            CreateIndexResponse indexResponse = client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
//            if(indexResponse.isAcknowledged()){
//                System.out.println("response id: "+ indexResponse.index());
//            }
//        } catch (IOException e) {
//            System.err.print(e.getStackTrace().toString());
////            System.out.println(e);
//        }


        /* JDBC connection 테스트 */

        try{
            String address = "jdbc:es://localhost:9200";
//            String address = "jdbc:elasticsearch://es1.danawa.io";
            Connection con = DriverManager.getConnection(address);

//            String user = "username";
//            String password = "password";
//            Connection con = DriverManager.getConnection(address, user, password);

            Statement st = con.createStatement();
            con.close();
            System.out.println("Connected!");
        }catch (SQLException e){
            System.out.println("Not Connected!");
            System.out.println(e);
        }

    }
}
