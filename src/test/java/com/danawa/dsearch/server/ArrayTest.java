package com.danawa.dsearch.server;

import java.util.ArrayList;
import java.util.List;

public class ArrayTest {


    public static void main(String[] args) {
        try {
            List<String> list = new ArrayList<>();

            list.get(0);
            System.out.println("조회 가능");
        } catch(IndexOutOfBoundsException e) {
            System.out.println("조회 불가");
        }

    }

}
