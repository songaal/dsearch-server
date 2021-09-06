package com.danawa.dsearch.server;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class JsonTest {
    private static Logger logger = LoggerFactory.getLogger(JsonTest.class);

    @Test
    public void aaa() {
        logger.warn("{}", 1);
    }

    public static int tracking(int[][] map, boolean[] check, int i){

        int n = map.length;

        if(n == i) return 0;

        int result = Integer.MAX_VALUE;
        for(int k = 0 ; k < n; k++){
            if(!check[k]){
                check[k] = true;
                result = Math.min(tracking(map, check, i+1), result) + map[i][k];
                check[k] = false;
            }
        }
        return result;
    }

    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int n = Integer.parseInt(br.readLine());
        if(n == 1){
            System.out.println(br.readLine());
        }

        int[][] map = new int[n][n];
        boolean[] check = new boolean[n];

        for(int i = 0 ; i < n ; i++){
            String[] split = br.readLine().split(" ");
            int j = 0;

            for(String num: split){
                map[i][j] = Integer.parseInt(num);
                j++;
            }
        }

        int result = Integer.MAX_VALUE;
        for(int k = 0 ; k < n; k++){
            check[k] = true;
            result = Math.min(result, tracking(map, check, 1) + map[0][k]);
            check[k] = false;
        }

        System.out.println(result);

        br.close();
    }

//    @Test
//    public void jsonvalidTest() {
//        String json = "";
//
//
//        try {
//            JSONObject jsonSchema = new JSONObject(json);
//            logger.debug("{}", jsonSchema);
//        } catch (JSONException e) {
//            e.printStackTrace();
//        }
////        JSONObject jsonSubject = new JSONObject(new JSONTokener(json));
////        Schema schema = SchemaLoader.load(jsonSchema);
////        schema.validate(jsonSubject);
//
//    }
}
