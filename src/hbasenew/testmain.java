/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import hbaseadmin.HBaseJavaAPI;
import hbasenew.file.FileSize;
import java.io.File;
import java.util.Map;
import java.util.function.BiConsumer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 *
 * @author 任强
 */
public class testmain {
    static   String tableName = "testearthauakedate";
    public static void main(String[] are) throws Exception{
      long start ,end;
     Hbasewrite  hw = new Hbasewrite();
        String path = ""; 
        
        
        start = System.nanoTime();
        hw.sendhbase(path,1);
        end = System.nanoTime();
        System.out.println("行键为台站测点测项采样率列为时间的写入时间: " + (end - start) / 1.0e9);
        
        
        start = System.nanoTime();
        hw.sendhbase(path,2);
        end = System.nanoTime();
        System.out.println("行键为台站测点测项采样率时间的写入时间: " + (end - start) / 1.0e9);

        Querymsg da =new Querymsg();
        da.setStation("53001");     
        da.setStarttime("201001010000"); 
        da.setEndtime("201001310000");
        da.setItem("4112");
        da.setStationpoint("2");
        da.setZoom("1");  
        
        
        start = System.nanoTime();
        StringBuilder result=hbaseread.query(da);
        end = System.nanoTime();
        result.length();
        System.out.println("行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
        
        
        start = System.nanoTime();
        result=hbaseread.query1(da);
        end = System.nanoTime();
        result.length();
        System.out.println("行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
       
        


    }
    
}
