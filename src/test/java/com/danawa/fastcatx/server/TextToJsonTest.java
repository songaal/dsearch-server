package com.danawa.fastcatx.server;

import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class TextToJsonTest {
    public static final String CHOSUNG_LIST = "ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ"; //19
    public static final String JUNGSUNG_LIST = "ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ"; //21
    public static final String JONGSUNG_LIST = " ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ"; //28
    private static final char unicodeHangulBase = '\uAC00';
    private static final char unicodeHangulLast = '\uD7A3';

    public static String makeHangulPrefix(String keyword, char delimiter) {
        StringBuffer candidate = new StringBuffer();
        StringBuffer prefix = new StringBuffer();
        for (int i = 0; i < keyword.length(); i++) {
            char ch = keyword.charAt(i);
            if (ch < unicodeHangulBase || ch > unicodeHangulLast) {
                prefix.append(ch);
                candidate.append(prefix);
            } else {
                // Character is composed of {Chosung+Jungsung} OR
                // {Chosung+Jungsung+Jongsung}
                int unicode = ch - unicodeHangulBase;
                int choSung = unicode / (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                // 1. add prefix+chosung
                candidate.append(prefix);
                candidate.append(CHOSUNG_LIST.charAt(choSung));
                candidate.append(delimiter);
                // 2. add prefix+chosung+jungsung
                unicode = unicode % (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                int jongSung = unicode % JONGSUNG_LIST.length();
                char choJung = (char) (ch - jongSung);
                candidate.append(prefix);
                candidate.append(choJung);
                // change prefix
                prefix.append(ch);
                if (jongSung > 0) {
                    candidate.append(delimiter);
                    // 3. add whole character
                    candidate.append(prefix);
                }
            }
            if (i < keyword.length() - 1)
                candidate.append(delimiter);
        }
        return candidate.toString();
    }


    public static String makeHangulChosung(String keyword, char delimiter) {
        StringBuffer candidate = new StringBuffer();
        StringBuffer prefix = new StringBuffer();
        for (int i = 0; i < keyword.length(); i++) {
            char ch = keyword.charAt(i);
            if (ch >= unicodeHangulBase && ch <= unicodeHangulLast) {
                int unicode = ch - unicodeHangulBase;
                int choSung = unicode / (JUNGSUNG_LIST.length() * JONGSUNG_LIST.length());
                candidate.append(prefix);
                candidate.append(CHOSUNG_LIST.charAt(choSung));

                if (i < keyword.length() - 1) {
                    candidate.append(delimiter);
                }

                prefix.append(CHOSUNG_LIST.charAt(choSung));
            }
        }
        return candidate.toString();
    }


    public static String makeSearchKeyword(String keyword){
        String[] keywordArray = keyword.split("[ \t\n\r]");
        StringBuilder sb = new StringBuilder();
        for(String splitKeyword : keywordArray) {
            sb.append(makeHangulPrefix(splitKeyword, ' '));
            sb.append(makeHangulChosung(splitKeyword, ' '));
        }
        return sb.toString();
    }

    public static void main(String[] args) throws IOException {
        String filePath = "C:\\Users\\admin\\Downloads\\AutoCompleteKeyword.dump.txt";
        String outFilePath = "C:\\Users\\admin\\Desktop\\AutoCompleteKeyword.json";

        File file = new File(outFilePath);
        FileWriter fw = null;
        Scanner scanner = null;

        long count = 0;

        boolean flag = false;
        boolean keywordFlag = false;
        boolean hitFlag = false;
        boolean rangeFlag = false;

        String keyword = null;
        String hit = null;
        String range = null;





        try{
            fw = new FileWriter(file, true);
            scanner = new Scanner(new File(filePath));
            while (scanner.hasNext()) {

                try {
                    String line = scanner.nextLine();
                    if("<doc>".equals(line)){
                        flag = true;
                    }else if("</doc>".equals(line)){
                        keyword = keyword.replace("\"", "").replace("\\", "");
                        String search = makeSearchKeyword(keyword).replace("\"", "").replace("\\", "");
//                    fw.write("{\"index\": {}}\n");
                        fw.write("{\"keyword\": \""+ keyword+ "\", \"hit\": "+ hit + ", \"range\": " + range + ", \"search\": \"" + search + "\"}\n");
                        count++;
                        if(count % 10000 == 0) System.out.println(count + "개 완료 되었습니다");
                        flag = false;
                    }else if("<KEYWORD>".equals(line)){
                        keywordFlag = true;
                    }else if("</KEYWORD>".equals(line)){
                        keywordFlag = false;
                    }else if("<HIT>".equals(line)){
                        hitFlag = true;
                    }else if("</HIT>".equals(line)){
                        hitFlag = false;
                    }else if("<RANGE>".equals(line)){
                        rangeFlag = true;
                    }else if("</RANGE>".equals(line)){
                        rangeFlag = false;
                    } else {
                        if(flag){
                            if(keywordFlag){
                                keyword = line;
                            }else if(hitFlag){
                                hit = line;
                            }else if(rangeFlag){
                                range = line;
                            }
                        }else{
                            // do nothing...
                        }
                    }

                } catch (Exception e) {
                    System.err.println("ERR : " + e.getMessage());
                }
            }

            if(flag){
                keyword = keyword.replace("\"", "").replace("\\", "");
                String search = makeSearchKeyword(keyword).replace("\"", "").replace("\\", "");
                fw.write("{\"keyword\": \""+ keyword+ "\", \"hit\": "+ hit + ", \"range\": " + range + ", \"search\": \"" + search + "\"}\n");
            }

        }catch (IOException e){
            e.printStackTrace();
        }finally {
            if(scanner != null){
                scanner.close();
            }
            if(fw != null){
                fw.close();
            }
        }
        System.out.println(count + "개 완료 되었습니다");
    }
}
