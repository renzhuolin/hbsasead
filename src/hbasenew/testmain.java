/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import hbasenew.file.Writelog;
import static hbasenew.file.Writelog.writeLog;

/**
 *
 * @author 任强
 */
public class testmain {

    public static void main(String[] are) throws Exception {
   // testdx ts = new testdx();
  //  ts.testwrite1(are);
   // ts.testwrite2(are);
   // ts.testread1(are);
    //ts.testread2(are);
          Querymsg da = new Querymsg();
           da.setStation("03002");
        da.setStarttime("201001231100");
          da.setEndtime("201001241200");
        da.setItem("4212");
        da.setStationpoint("1");
        da.setZoom("0");
       StringBuilder   result = hbaseread.query4(da);
       System.out.print(result);
    }
    
}


