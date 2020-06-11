package com.danawa.fastcatx.server;

import com.google.gson.Gson;

import java.io.*;
import java.util.*;

public class DictParsingTest {

     private enum TYPE { USER, SYNONYM, STOP, SPACE, COMPOUND, UNIT, UNIT_SYNONYM, MAKER, BRAND, CATEGORY, ENGLISH }


    public static void main(String[] args) {
//        데이터 확인후..
//        curl -s -H "Content-Type: application/x-ndjson" -XPOST http://es1.danawa.io/.fastcatx_dict/_bulk --data-binary @user.json

        UserDict("C:\\Users\\admin\\Downloads\\user.txt", "C:\\Users\\admin\\Downloads\\user.json");
        synonym("C:\\Users\\admin\\Downloads\\synonym.txt", "C:\\Users\\admin\\Downloads\\synonym.json");
        stop("C:\\Users\\admin\\Downloads\\stop.txt", "C:\\Users\\admin\\Downloads\\stop.json");
        space("C:\\Users\\admin\\Downloads\\space.txt", "C:\\Users\\admin\\Downloads\\space.json");
        compound("C:\\Users\\admin\\Downloads\\compound.txt", "C:\\Users\\admin\\Downloads\\compound.json");
        unit("C:\\Users\\admin\\Downloads\\unit.txt", "C:\\Users\\admin\\Downloads\\unit.json");
        unitSynonym("C:\\Users\\admin\\Downloads\\unit_synonym.txt", "C:\\Users\\admin\\Downloads\\unit_synonym.json");
        maker("C:\\Users\\admin\\Downloads\\maker.txt", "C:\\Users\\admin\\Downloads\\maker.json");
        brand("C:\\Users\\admin\\Downloads\\brand.txt", "C:\\Users\\admin\\Downloads\\brand.json");
        category("C:\\Users\\admin\\Downloads\\category.txt", "C:\\Users\\admin\\Downloads\\category.json");
        english("C:\\Users\\admin\\Downloads\\english.txt", "C:\\Users\\admin\\Downloads\\english.json");


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
                dict.setKeyword(column[0]);
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
                dict.setId(column[0]);
                dict.setKeyword(column[1]);
                dict.setSynonym(column[2]);
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
                dict.setId(column[0]);
                dict.setKeyword(column[1]);
                dict.setSynonym(column[2]);
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
                dict.setId(column[0]);
                dict.setKeyword(column[1]);
                dict.setSynonym(column[2]);
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
                dict.setSynonym(column[0]);
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
                dict.setKeyword(column[0]);
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
                dict.setKeyword(column[0]);
                dict.setSynonym(column[1]);
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
                dict.setKeyword(column[0]);
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
                dict.setKeyword(column[0]);
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
                dict.setKeyword(column[0]);
                dict.setSynonym(column[1]);
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
                dict.setKeyword(column[0]);
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
