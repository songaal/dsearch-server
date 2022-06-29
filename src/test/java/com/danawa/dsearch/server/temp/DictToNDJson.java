package com.danawa.dsearch.server.temp;

import com.google.gson.Gson;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class DictToNDJson {

    public static void main(String[] args) {
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @user.json


        String path = "C:\\Users\\admin\\Downloads";

        convert(path + "\\brand.txt", path + "\\brand.json");


    }

    public static void convert(String input, String output) {
        Scanner scanner = null;
        BufferedWriter writer = null;
        try {
            Map<String, Object> metaLine = new HashMap<>();
            metaLine.put("index", new HashMap<>());
            scanner = new Scanner(new File(input));
            writer = new BufferedWriter(new FileWriter(output));
            writer.write("");

            while (scanner.hasNext()) {
                String line = scanner.nextLine();
                String[] column = line.split("\t");



                //
                if (column.length == 3) {
                    Map<String, String> data = new HashMap<>();
                    if (column[2].contains(",")) {
                        continue;
                    }

                    data.put("title", column[2].trim());
                    writer.append(new Gson().toJson(metaLine) + "\r\n");
                    writer.append(new Gson().toJson(data) + "\r\n");
                }


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
