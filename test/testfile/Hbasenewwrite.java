package testfile;

import hbasenew.HBaseUtil;
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

    private String tableName = "tearth";
    private List<Put> puts = new ArrayList<>();

    private String filename = "F:\\固态\\earth";
    private String rate = null;

    private Put put = null;

    public void input() throws Exception {
        File file = new File(getFilename());
        File[] fd = file.listFiles();
        BufferedReader reader = null;
        for (int i = 0; i < fd.length; i++) {
            file = fd[i];
            String st = file.getName().substring(0, 12);
            String[] s = st.split("_");
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                if ((tempString = reader.readLine()) != null) {
                    String[] str = tempString.split(" ");

                    if (str[0].length() == 12) {
                        setRate("1");
                        //  Timestamp ts = new Timestamp(System.currentTimeMillis());
                        //String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
                        //	ts = Timestamp.valueOf(strr); 
                        put = new Put(Bytes.toBytes(s[0] + s[1] + s[2] + rate));//参数是行键台站+测点+测向+采样率
                        setPut(new Put(Bytes.toBytes(s[0] + s[1] + s[2] + getRate() + str[0])));//参数是行键台站+测点+测向+采样率+时间
                        //  put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));
                        // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
                        getPut().addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                        HBaseUtil.inserts(getTableName(), getPut());
                    }
                }

                while ((tempString = reader.readLine()) != null) {
                    String[] str = tempString.split(" ");
                    if (str[0].length() == 12) {
                        //	Timestamp ts = new Timestamp(System.currentTimeMillis());
                        setRate("1");
                        //String strr = str[0].substring(0, 4)+"-"+str[0].substring(4, 6)+"-"+str[0].substring(6, 8)+" "+str[0].substring(8, 10)+":"+str[0].substring(10, 12)+":"+"00";
                        //ts = Timestamp.valueOf(strr); 						
                        // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(Long.toString(ts.getTime())), Bytes.toBytes(str[1]));           
                        setPut(new Put(Bytes.toBytes(s[0] + s[1] + s[2] + getRate() + str[0])));
                        getPut().addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                        HBaseUtil.inserts(getTableName(), getPut());
                        // put.addColumn(Bytes.toBytes("date"), Bytes.toBytes(str[0]), Bytes.toBytes(str[1]));
//                                                if(put!=null&&put.size()==100000)
//                                                {
//                                                 System.out.println(HBaseUtil.inserts(tableName, put));
//                                                put= new Put(Bytes.toBytes( s[0]+s[1]+s[2]+rate));
//                                                }
                    } else if (str[0].length() == 10) {
                        Timestamp ts = new Timestamp(System.currentTimeMillis());
                        String strr = str[0].substring(0, 4) + "-" + str[0].substring(4, 6) + "-" + str[0].substring(6, 8) + " " + str[0].substring(8, 10) + ":00:00";
                        ts = Timestamp.valueOf(strr);
                        System.out.print(s[0] + " " + s[1] + " " + s[2] + " ");
                        System.out.println(Long.toString(ts.getTime()) + " " + str[1]);
				 if(put!=null&&put.size()==10000)
                                               
                               {
                                                System.out.println(HBaseUtil.inserts(tableName, put));
                                               put = null;
                       }
                    }
                }
                                 if(put!=null){
                               boolean asdas= HBaseUtil.inserts(tableName, put);
                            System.out.println(asdas);
                System.out.println(i);
                setPut(null);
                                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Congratulation~");
    }

    public void input1() throws Exception {
        File file = new File(getFilename());
        File[] fd = file.listFiles();
        BufferedReader reader = null;
        for (int i = 0; i < fd.length; i++) {
            file = fd[i];
            String st = file.getName().substring(0, 12);
            String[] s = st.split("_");
            try {
                reader = new BufferedReader(new FileReader(file));
                String tempString = null;
                if ((tempString = reader.readLine()) != null) {
                    String[] str = tempString.split(" ");
                    if (str[0].length() == 12) {
                        setRate("1");
                        setPut(new Put(Bytes.toBytes(s[0] + s[1] + s[2] + getRate() + str[0])));//参数是行键台站+测点+测向+采样率+时间                      
                        getPut().addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                        getPuts().add(getPut());
                    }
                }
                while ((tempString = reader.readLine()) != null) {
                    String[] str = tempString.split(" ");
                    if (str[0].length() == 12) {
                        setRate("1");
                        setPut(new Put(Bytes.toBytes(s[0] + s[1] + s[2] + getRate() + str[0])));
                        getPut().addColumn(Bytes.toBytes("data"), Bytes.toBytes("value"), Bytes.toBytes(str[1]));
                        getPuts().add(getPut());

                        if (getPuts() != null && getPuts().size() == 100000) {
                            System.out.println(HBaseUtil.inserts(getTableName(), getPuts()));
                            getPuts().clear();
                        }
                    } else if (str[0].length() == 10) {
                        Timestamp ts = new Timestamp(System.currentTimeMillis());
                        String strr = str[0].substring(0, 4) + "-" + str[0].substring(4, 6) + "-" + str[0].substring(6, 8) + " " + str[0].substring(8, 10) + ":00:00";
                        ts = Timestamp.valueOf(strr);
                        System.out.print(s[0] + " " + s[1] + " " + s[2] + " ");
                        System.out.println(Long.toString(ts.getTime()) + " " + str[1]);
                        if (getPut() != null && getPut().size() == 10000) {
                            System.out.println(HBaseUtil.inserts(getTableName(), getPuts()));
                            setPut(null);
                        }
                    }
                }
                if (getPuts() != null) {
                    boolean asdas = HBaseUtil.inserts(getTableName(), getPuts());
                    System.out.println(asdas);
                    System.out.println(i);
                    getPuts().clear();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Congratulation~");
    }

    /**
     * @return the tableName
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * @param tableName the tableName to set
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
     * @return the puts
     */
    public List<Put> getPuts() {
        return puts;
    }

    /**
     * @param puts the puts to set
     */
    public void setPuts(List<Put> puts) {
        this.puts = puts;
    }

    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the rate
     */
    public String getRate() {
        return rate;
    }

    /**
     * @param rate the rate to set
     */
    public void setRate(String rate) {
        this.rate = rate;
    }

    /**
     * @return the put
     */
    public Put getPut() {
        return put;
    }

    /**
     * @param put the put to set
     */
    public void setPut(Put put) {
        this.put = put;
    }

}
