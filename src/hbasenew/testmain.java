/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import hbaseadmin.HBaseJavaAPI;
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
//      Hbasenewwrite  hw = new Hbasenewwrite();
//       hw.input1();
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



Querymsg da =new Querymsg();
   
//     System.out.println("结果是的： " + result.size());
//    System.out.println("结果是的： " + result);
     
        
        da.setStation("53001");
      
        da.setStarttime("201001010000"); 
          da.setEndtime("201010310000");
        da.setItem("4112");
        da.setStationpoint("2");
        da.setZoom("1");     
        Map<String, String> result=hbaseread.query(da);
         System.out.println("结果是的： " + result);
         
      
        assertNotNull(result);
    }
    
}
