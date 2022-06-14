package com.danawa.dsearch.server.temp;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.*;
import java.util.*;

public class ACIndexingTest {

    private static List<String> failList = new ArrayList<>();

    public static void main(String[] args) {
        String filePath = "C:\\Users\\admin\\Downloads\\AccruePopularKeyword\\AccruePopularKeyword.txt";

        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 9200, "http"));
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);

        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(filePath));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        int count = 0;
        BulkRequest request = new BulkRequest();
        List<String> tmpFailList = new ArrayList<>();
        while (scanner.hasNext()) {
            try {
                String line = scanner.nextLine();
                tmpFailList.add(line);

                String[] split = line.split("\t");
                if (split.length == 4) {

                    Map<String, Object> source = new HashMap<>();
                    source.put("item", split[0]);
                    source.put("hits", split[1]);
                    source.put("YN", split[2]);
                    source.put("keyword", split[3]);

                    request.add(new IndexRequest(".ac_index").source(source));
                } else {
                    failList.add(line);
                }

                if ((count % 10000) == 0) {
                    client.bulk(request, RequestOptions.DEFAULT);
                    request = new BulkRequest();
                    tmpFailList.clear();
                    count = 0;
                } else {
                    count++;
                }
            } catch (Exception e) {
                failList.addAll(tmpFailList);
                System.err.println("ERROR Bulk" + e.getMessage());
            }
        }

        if (tmpFailList.size() != 0) {
            try {
                client.bulk(request, RequestOptions.DEFAULT);
            } catch (Exception e) {
                failList.addAll(tmpFailList);
                System.err.println("ERROR Bulk" + e.getMessage());
            }
        }

        failList.forEach(s -> {
            System.out.println(" >>" + s);
        });
    }


//    public static void main(String[] args) throws IOException {
//        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
////        RestClientBuilder builder = RestClient.builder(new HttpHost("es1.danawa.io", 9200, "http"));
//        RestHighLevelClient client = new RestHighLevelClient(builder);
//
//        String filePath = "C:\\Users\\admin\\Downloads\\AccruePopularKeyword\\AccruePopularKeyword.txt";
//
//        File file =  new File(filePath);
//
//        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
//        BulkRequest request = new BulkRequest();
//        request.timeout(TimeValue.timeValueMinutes(5));
//        request.timeout("5m");
//        long count = 0;
//        String line = null;
//        BulkResponse bulkResponse = null;
//        while((line = bufferedReader.readLine()) != null){
//            /* 파싱 */
//            String[] split = line.split("\t");
//            if(split.length < 4){
//                request.add(new IndexRequest(".ac_index")
//                        .source(XContentType.JSON,"item", split[0])
//                        .source(XContentType.JSON,"hits", split[1])
//                        .source(XContentType.JSON,"YN", split[2])
//                        .source(XContentType.JSON,"keyword", "")
//                );
//            }else if(split.length < 3){
//                request.add(new IndexRequest(".ac_index")
//                        .source(XContentType.JSON,"item", split[0])
//                        .source(XContentType.JSON,"hits", split[1])
//                        .source(XContentType.JSON,"YN", "")
//                        .source(XContentType.JSON,"keyword", "")
//                );
//            }else if(split.length < 2){
//                request.add(new IndexRequest(".ac_index")
//                        .source(XContentType.JSON,"item", split[0])
//                        .source(XContentType.JSON,"hits", "")
//                        .source(XContentType.JSON,"YN", "")
//                        .source(XContentType.JSON,"keyword", "")
//                );
//            }else{
//                request.add(new IndexRequest(".ac_index")
//                        .source(XContentType.JSON,"item", split[0])
//                        .source(XContentType.JSON,"hits", split[1])
//                        .source(XContentType.JSON,"YN", split[2])
//                        .source(XContentType.JSON,"keyword", split[3])
//                );
//            }
//
//            count++;
//            if(count % 5000 == 0) {
//                bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//                request = new BulkRequest();
//                System.out.println("count : " + count + ", status : " + bulkResponse.status().getStatus());
//            }
//        }
//
//        bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
//        System.out.println("count : " + count + ", status : " + bulkResponse.status().getStatus());
//        System.out.println("끝났습니다");
//        bufferedReader.close();
//        client.close();
//    }
}
