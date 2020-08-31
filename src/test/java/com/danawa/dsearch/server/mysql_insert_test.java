package com.danawa.dsearch.server;


import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class mysql_insert_test {
    public static void main(String[] args) {
        BufferedReader br = null;
        Connection con = null;
        PreparedStatement pstmt = null;
        String filepath = "C:\\Users\\admin\\Desktop\\csv\\merge.csv";

        try{
            con = DriverManager.getConnection("jdbc:mysql://kube1.danawa.io:30009/index_test","root", "ekskdhk");
            con.setAutoCommit(false);
            String driver = "com.mysql.jdbc.Driver";

            br = Files.newBufferedReader(Paths.get(filepath));
            Class.forName(driver);
            String line = "";
            boolean flag = true;
            System.out.println("start!");
            long count = 0;
            String SQL = "insert into tcTable(human_name, email, registered_date, company, pass) values(?, ?, ?, ?, ?)";
            pstmt = con.prepareStatement(SQL);

            while((line = br.readLine()) != null){
                if(flag) {
                    flag = false;
                    continue;
                }
                String array[] = line.split(",");
                for(int i = 0 ;i < 5; i++){
                    if("".equals(array[i])){
                        continue;
                    }
                    pstmt.setString(i+1, array[i]);
                }
                pstmt.addBatch();
                pstmt.clearParameters() ;
                count++;
                if(count % 10000 == 0) {
                    pstmt.executeBatch() ;
                    pstmt.clearBatch();
                    con.commit();
                    System.out.println("" + count + "개 끝났습니다.");
                }
            }
            pstmt.executeBatch() ;
            pstmt.clearBatch();
            con.commit() ;
        }catch(FileNotFoundException e){
            e.printStackTrace();
        }catch(IOException e){
            e.printStackTrace();
        }catch (SQLException e){
            e.printStackTrace();
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }finally{
            try{
                if(br != null){
                    br.close();
                }
            }catch(IOException e){
                e.printStackTrace();
            }

            if (pstmt != null) { try { pstmt.close(); } catch (SQLException e) { e.printStackTrace(); } } if (con != null) { try { con.close(); } catch (SQLException e) { e.printStackTrace(); } }
        }
    }
}
