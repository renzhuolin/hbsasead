package hbasenew;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbasewrite {

    public void Post(File file, String url, int x) throws InterruptedException, IOException {
        String tempString = null;
        int rate;
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String st = file.getName().substring(0, 12);
        String[] s = st.split("_");

        if (x == 1) {
            try {
                if ((tempString = reader.readLine()) == null) {
                    System.out.println("文件为空");
                } else {
                    String[] str = tempString.split(" ");
                    switch (str[0].length()) {
                        case 12:
                            rate = 1;
                            String msg = s[0] + s[1] + s[2] + rate;
                            Write1(msg,tempString,reader);
                            break;
                        case 10:
                            rate = 2;
                            break;
                        case 8:
                            rate = 3;
                            break;
                        case 14:
                            rate = 0;
                            break;
                        default:
                            break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else if (x == 2) {
            try {
                while ((tempString = reader.readLine()) != null) {
                    String[] str = tempString.split(" ");
                    switch (str[0].length()) {
                        case 12: {
                            rate = 1;
                            break;
                        }
                        case 10: {
                            rate = 2;
                            break;
                        }
                        case 8: {
                            rate = 3;
                            break;
                        }
                        case 14: {
                            rate = 0;
                            break;
                        }
                        default:
                            break;
                    }
                }

            } catch (IOException e) {

                e.printStackTrace();
            }
        }
    }

    /*
    参数是行键台站+测点+测向+采样率
     */
    public boolean Write1(String rate, String tempString, BufferedReader reader) {
        String TableName = "testearthauakedate";
        String[] str = tempString.split(" ");
        String strr = str[0].substring(0, 4) + "-" + str[0].substring(4, 6) + "-" + str[0].substring(6, 8) + " " + str[0].substring(8, 10) + ":" + str[0].substring(10, 12) + ":" + "00";
        Put put = new Put(Bytes.toBytes(rate));
        Timestamp ts = Timestamp.valueOf(strr);
        put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
        try {
            while ((tempString = reader.readLine()) != null) {
                str = tempString.split(" ");
                put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                if (!put.isEmpty() && put.size() == 100000) {
                    System.out.println(HBaseUtil.inserts(TableName, put));
                    put = new Put(Bytes.toBytes(rate));
                }
            }
            if (!put.isEmpty()) {
                System.out.println(HBaseUtil.inserts(TableName, put));
                put = null;
            }
        } catch (IOException ex) {
            Logger.getLogger(Hbasewrite.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
     public boolean Write2(String rate, String tempString, BufferedReader reader) {
        String TableName = "testearthauakedate";
        String[] str = tempString.split(" ");
        String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":00:00";
        Put put = new Put(Bytes.toBytes(rate));
        Timestamp ts = Timestamp.valueOf(strr);
        put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
        try {
            while ((tempString = reader.readLine()) != null) {
                str = tempString.split(" ");
                put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                if (!put.isEmpty() && put.size() == 100000) {
                    System.out.println(HBaseUtil.inserts(TableName, put));
                    put = new Put(Bytes.toBytes(rate));
                }
            }
            if (!put.isEmpty()) {
                System.out.println(HBaseUtil.inserts(TableName, put));
                put = null;
            }
        } catch (IOException ex) {
            Logger.getLogger(Hbasewrite.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
      public boolean Write3(String rate, String tempString, BufferedReader reader) {
        String TableName = "testearthauakedate";
        String[] str = tempString.split(" ");
        String strr = str[0].substring(0, 4) + "-" + str[0].substring(4, 6) + "-" + str[0].substring(6, 8) + " " + str[0].substring(8, 10) + ":" + str[0].substring(10, 12) + ":" + "00";
        Put put = new Put(Bytes.toBytes(rate));
        Timestamp ts = Timestamp.valueOf(strr);
        put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
        try {
            while ((tempString = reader.readLine()) != null) {
                str = tempString.split(" ");
                put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                if (!put.isEmpty() && put.size() == 100000) {
                    System.out.println(HBaseUtil.inserts(TableName, put));
                    put = new Put(Bytes.toBytes(rate));
                }
            }
            if (!put.isEmpty()) {
                System.out.println(HBaseUtil.inserts(TableName, put));
                put = null;
            }
        } catch (IOException ex) {
            Logger.getLogger(Hbasewrite.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }
       public boolean Write0(String rate, String tempString, BufferedReader reader) {
        String TableName = "testearthauakedate";
        String[] str = tempString.split(" ");
        String strr = str[0].substring(0, 4) + "-" + str[0].substring(4, 6) + "-" + str[0].substring(6, 8) + " " + str[0].substring(8, 10) + ":" + str[0].substring(10, 12) + ":" + "00";
        Put put = new Put(Bytes.toBytes(rate));
        Timestamp ts = Timestamp.valueOf(strr);
        put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
        try {
            while ((tempString = reader.readLine()) != null) {
                str = tempString.split(" ");
                put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                if (!put.isEmpty() && put.size() == 100000) {
                    System.out.println(HBaseUtil.inserts(TableName, put));
                    put = new Put(Bytes.toBytes(rate));
                }
            }
            if (!put.isEmpty()) {
                System.out.println(HBaseUtil.inserts(TableName, put));
                put = null;
            }
        } catch (IOException ex) {
            Logger.getLogger(Hbasewrite.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

}
