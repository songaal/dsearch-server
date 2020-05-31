package com.danawa.fastcatx.server;

import com.google.gson.Gson;
import org.assertj.core.util.TextFileWriter;

import java.io.*;
import java.util.*;

public class DictParsingTest {

    private enum TYPE { USER, SYNONYM, STOP, SPACE, COMPOUND, UNIT, UNIT_SYNONYM, MAKER, BRAND, CATEGORY, ENGLISH }

    public static void main(String[] args) {


        UserDict();


    }


    public static void UserDict() {
        Scanner scanner = null;
        BufferedWriter writer = null;

        try {
            String dictFile = "D:\\다나와 검색엔진\\테스트 서버 마이그레이션 파일\\user.txt";
            String outputFile = "D:\\다나와 검색엔진\\테스트 서버 마이그레이션 파일\\user.json";

            Map<String, Object> metaLine = new HashMap<>();
            metaLine.put("index", new HashMap<>());

            scanner = new Scanner(new File(dictFile));
            writer = new BufferedWriter(new FileWriter(outputFile));

            writer.write("");


            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] column = line.split("\t");
                DictEntityTest dict = new DictEntityTest();
                dict.setKeyword(column[0]);
                dict.setType(TYPE.USER.toString());
                writer.append(new Gson().toJson(metaLine) + "\r\n");
                writer.append(new Gson().toJson(dict) + "\r\n");
            }
            writer.flush();
            System.out.println("finished");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}
