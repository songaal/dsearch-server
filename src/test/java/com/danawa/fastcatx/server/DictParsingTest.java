package com.danawa.fastcatx.server;

import com.google.gson.Gson;
import org.assertj.core.util.TextFileWriter;

import java.io.*;
import java.util.*;

public class DictParsingTest {

    public static void main(String[] args) {

//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.fastcatx_dict/_bulk --data-binary @user.json
        UserDict();


    }


    public static void UserDict() {
        Scanner scanner = null;
        BufferedWriter writer = null;

        try {
            String dictFile = "C:\\Users\\admin\\Downloads\\user.txt";
            String outputFile = "C:\\Users\\admin\\Downloads\\user.json";

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
                dict.setType("USER");
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
