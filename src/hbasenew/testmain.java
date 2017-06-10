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
        
     Hbasewrite  hw = new Hbasewrite();
      hw.sendhbase(tableName, 0);
       //  HBaseJavaAPI.getAllRows(tableName);
   //   HBaseUtil.getRow(tableName, "53001241121");
//   long ti;
//   for(ti=128107950;ti<128158970;ti += 6)
//   {
//        String result = HBaseUtil.byGet(tableName, "53001241121", "date",Long.toString(ti)+"0000");
//       // System.out.println("结果是的： " + result);
//       
//   }
//    Map<String, String> result = HBaseUtil.byGet(tableName, "53001241121", "date");
//        System.out.println("结果是的： " + result);



//Querymsg da =new Querymsg();
//   
////     System.out.println("结果是的： " + result.size());
////    System.out.println("结果是的： " + result);
//     
//        
//        da.setStation("53001");
//      
//        da.setStarttime("201001010000"); 
//          da.setEndtime("201001310000");
//        da.setItem("4112");
//        da.setStationpoint("2");
//        da.setZoom("1");     
//        StringBuilder result=hbaseread.query(da);
//       System.out.println("结果是的： " + result);
       
//        result.entrySet().stream().forEach((entry) -> {
//            System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
//        });  
//        assertNotNull(result);
FileSize ss = new FileSize();
ss.getfilesize("c:\\");

    }
    
}
