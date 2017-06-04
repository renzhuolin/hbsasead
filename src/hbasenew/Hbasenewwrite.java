package hbasenew;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbasenewwrite {
//String tableName = "testearthauakedate";
    String tableName = "tearth";
    List<Put> puts =new ArrayList<>();
   
       private String filename ="F:\\固态\\earth";
        String  rate  = null;
       
         Put put = null;
     public void input() throws Exception {
       
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
				if((tempString = reader.readLine()) != null)
                                {
                                String []str= tempString.split(" ");
                               
					if (str[0].length()==12){
                                             rate = "1";
                                           //  Timestamp ts = new Timestamp(System.currentTimeMillis());
						//String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
					//	ts = Timestamp.valueOf(strr); 
                                                //put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate));//参数是行键台站+测点+测向+采样率
                                                 put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate+str[0]));//参数是行键台站+测点+测向+采样率+时间
                                              //  put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
                                               // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                                                 HBaseUtil.inserts(tableName,put);
                                        }                                        
                                }
                                
                               
				while((tempString = reader.readLine()) != null){
                                       String []str= tempString.split(" ");
					if (str[0].length()==12){
					//	Timestamp ts = new Timestamp(System.currentTimeMillis());
                                             rate = "1";
						//String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
						//ts = Timestamp.valueOf(strr); 						
						// put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));           
						put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate+str[0]));
                                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                                                   HBaseUtil.inserts(tableName,put);
                                               // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
//                                                if(put!=null&&put.size()==100000)
//                                                {
//                                                 System.out.println(HBaseUtil.inserts(tableName, put));
//                                                put= new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate));
//                                                }
                                        }
						
					else if (str[0].length()==10){
						Timestamp ts = new Timestamp(System.currentTimeMillis());
						String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":00:00";
						ts = Timestamp.valueOf(strr);
						System.out.print(s[0]+" "+s[1]+" "+s[2]+" ");
						System.out.println(Long.toString(ts.getTime())+" "+str[1]);												 
//				 if(put!=null&&put.size()==10000)
//                                                
//                                 {
//                                                 System.out.println(HBaseUtil.inserts(tableName, put));
//                                                 put = null;
//                                                }
					}
				}
//                                 if(put!=null){
//                               boolean asdas= HBaseUtil.inserts(tableName, put);
//                               System.out.println(asdas);
                                System.out.println(i);
                                put= null;
//                                 }
                                     
                                    
                                    
						
			} catch (Exception e) {				
				e.printStackTrace();
			}
		}
		System.out.println("Congratulation~");
     }
      public void input1() throws Exception {
       
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
				if((tempString = reader.readLine()) != null)
                                {
                                String []str= tempString.split(" ");
                               
					if (str[0].length()==12){
                                             rate = "1";
                                           //  Timestamp ts = new Timestamp(System.currentTimeMillis());
						//String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
					//	ts = Timestamp.valueOf(strr); 
                                                //put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate));//参数是行键台站+测点+测向+采样率
                                                 put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate+str[0]));//参数是行键台站+测点+测向+采样率+时间
                                              //  put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
                                               // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                                                puts.add(put);
                                        }                                        
                                }
                                
                             
				while((tempString = reader.readLine()) != null){
                                       String []str= tempString.split(" ");
					if (str[0].length()==12){
					//	Timestamp ts = new Timestamp(System.currentTimeMillis());
                                             rate = "1";
						//String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
						//ts = Timestamp.valueOf(strr); 						
						// put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));           
						put =new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate+str[0]));
                                                put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                                                 puts.add(put);
                                               // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                                              if(puts!=null&&puts.size()==100000)
                                                {
                                                 System.out.println(HBaseUtil.inserts(tableName, puts));
                                               puts.clear();
                                                }
                                        }
						
					else if (str[0].length()==10){
						Timestamp ts = new Timestamp(System.currentTimeMillis());
						String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":00:00";
						ts = Timestamp.valueOf(strr);
						System.out.print(s[0]+" "+s[1]+" "+s[2]+" ");
						System.out.println(Long.toString(ts.getTime())+" "+str[1]);												 
				 if(put!=null&&put.size()==10000)
                                               
                                {
                                                 System.out.println(HBaseUtil.inserts(tableName, puts));
                                                put = null;
                                               }
					}
				}
                                if(puts!=null){
                               boolean asdas= HBaseUtil.inserts(tableName, puts);
                               System.out.println(asdas);
                                System.out.println(i);
                                puts.clear();
                                }
                                     
                                    
                                    
						
			} catch (Exception e) {				
				e.printStackTrace();
			}
		}
		System.out.println("Congratulation~");
     }

}