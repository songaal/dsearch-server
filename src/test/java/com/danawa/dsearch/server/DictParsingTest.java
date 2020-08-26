package com.danawa.dsearch.server;

import com.google.gson.Gson;

import java.io.*;
import java.util.*;

public class DictParsingTest {

     private enum TYPE { USER, SYNONYM, STOP, SPACE, COMPOUND, UNIT, UNIT_SYNONYM, MAKER, BRAND, CATEGORY, ENGLISH }


    public static void main(String[] args) {
//        데이터 확인후..
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @user.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @synonym.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @stop.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @space.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @compound.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @unit.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @unit_synonym.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @maker.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @brand.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @category.json
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.dsearch_dict/_bulk --data-binary @english.json


        String path = "C:\\Users\\admin\\Downloads\\Downloads";

        UserDict(path + "\\user.txt", path + "\\user.json");
        synonym(path + "\\synonym.txt", path + "\\synonym.json");
        stop(path + "\\stop.txt", path + "\\stop.json");
        space(path + "\\space.txt", path + "\\space.json");
        compound(path + "\\compound.txt", path + "\\compound.json");
        unit(path + "\\unit.txt", path + "\\unit.json");
        unitSynonym(path + "\\unit_synonym.txt", path + "\\unit_synonym.json");
        maker(path + "\\maker.txt", path + "\\maker.json");
        brand(path + "\\brand.txt", path + "\\brand.json");
        category(path + "\\category.txt", path + "\\category.json");
        english(path + "\\english.txt", path + "\\english.json");


    }

    public static void english(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.ENGLISH.name());
                dict.setKeyword(column[0].trim());
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

    public static void category(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.CATEGORY.name());
                dict.setId(column[0].trim());
                dict.setKeyword(column[1].trim());
                dict.setValue(column[2].trim());
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
    public static void brand(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.BRAND.name());
                dict.setId(column[0].trim());
                dict.setKeyword(column[1].trim());
                dict.setValue(column[2].trim());
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

    public static void maker(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.MAKER.name());
                dict.setId(column[0].trim());
                dict.setKeyword(column[1].trim());
                dict.setValue(column[2].trim());
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

    public static void unitSynonym(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.UNIT_SYNONYM.name());
                dict.setValue(column[0].trim());
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

    public static void unit(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.UNIT.name());
                dict.setKeyword(column[0].trim());
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

    public static void compound(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.COMPOUND.name());
                dict.setKeyword(column[0].trim());
                dict.setValue(column[1].trim());
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

    public static void space(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.SPACE.name());
                dict.setKeyword(column[0].trim());
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

    public static void stop(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.STOP.name());
                dict.setKeyword(column[0].trim());
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

    public static void synonym(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setType(TYPE.SYNONYM.name());
                dict.setKeyword(column[0].trim());
                dict.setValue(column[1].trim());
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

    public static void UserDict(String input, String output) {
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
                DictEntityTest dict = new DictEntityTest();
                dict.setKeyword(column[0].trim());
                dict.setType(TYPE.USER.name());
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
