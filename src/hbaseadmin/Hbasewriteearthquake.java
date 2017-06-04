/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbaseadmin;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
/**
 *
 * @author 任强
 */

public class Hbasewriteearthquake {
    HBaseJavaAPI hb = new HBaseJavaAPI();
      String tableName = "testearthauakedate";
       private String filename ="F:\\固态\\earth";
        String  rate  = null;
            // 第一步：创建数据库表：“testearthauakedate”
      public  void input(String[] args) throws Exception {
		// TODO Auto-generated method stub
//                   String[] columnFamilys = { "date" };
//          hb.createTable(tableName, columnFamilys);
		File file = new File(filename);
		File[] fd=file.listFiles();
		BufferedReader reader = null;
		for (int i=0;i<fd.length;i++){
			file=fd[i];
			String st = file.getName().substring(0,12);
			String[] s=st.split("_");
		
			try {
				reader = new BufferedReader(new FileReader(file));
				String tempString = null;
				//new HttpRequest("http://"+url+"/api/put");
				String sr ="[ ";
				int t=0;
				while((tempString = reader.readLine()) != null){
					String []str= tempString.split(" ");
					if (str[0].length()==12){
						Timestamp ts = new Timestamp(System.currentTimeMillis());
                                             rate = "1";
						String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
						ts = Timestamp.valueOf(strr);  
                                                System.out.println(strr);
						//System.out.println(s[0]+s[1]+s[2]+rate);
                                               
						//System.out.println(Long.toString(ts.getTime())+" "+str[1]);
						
						 if (hb.isExist(tableName)) {              
                                          hb.addRow(tableName, s[0]+s[1]+s[2]+rate, "date",Long.toString(ts.getTime()), str[1]);                                       
                                                } else {
                                                System.out.println(tableName + "此数据库表不存在！");	
		
							
						}
                                        }
						
					else if (str[0].length()==10){
						Timestamp ts = new Timestamp(System.currentTimeMillis());
						String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":00:00";
						ts = Timestamp.valueOf(strr);
						System.out.print(s[0]+" "+s[1]+" "+s[2]+" ");
						System.out.println(Long.toString(ts.getTime())+" "+str[1]);												 
						sr +="{\"metric\":\""+s[2]+"\", \"timestamp\":"+Long.toString(ts.getTime())+", \"value\":"+str[1]+", \"tags\":{\"rate\":\""+s[1]+"\", \"station\":\""+s[0]+"\"}},";
					
					}
				}			
						
			} catch (Exception e) {				
				e.printStackTrace();
			}
		}
		System.out.println("Congratulation~");
	}
      public void writebase() throws Exception{ 
            String[] columnFamilys = { "date" };
          hb.createTable(tableName, columnFamilys);
           if (hb.isExist(tableName)) {
               
                HBaseJavaAPI.addRow(tableName, "zpc", "info", "age", "20");
                HBaseJavaAPI.addRow(tableName, "zpc", "info", "sex", "boy");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "china", "97");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "math", "128");
                HBaseJavaAPI.addRow(tableName, "zpc", "course", "english", "85");
            } else {
                System.out.println(tableName + "此数据库表不存在！");
    
      }
}
     
}
