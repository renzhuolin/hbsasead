package hbasenew;

import static com.kenai.jaffl.provider.jffi.CodegenUtils.c;
import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import static org.jruby.util.CodegenUtils.c;

public class HBaseUtil {
    private static Configuration conf;
    private static Connection con;
    // 初始化连接
    static {
        conf = HBaseConfiguration.create(); // 获得配制文件对象
      conf.set("hbase.zookeeper.quorum", "bigdata-1,bigdata-2,bigdata-3,bigdata-4,bigdata-5,bigdata-6");
    // conf.addResource("hbase-site.xml");
    
        try {
            con = ConnectionFactory.createConnection(conf);// 获得连接对象
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 获得连接
    public static Connection getCon() {
        if (con == null || con.isClosed()) {
            try {
                con = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return con;
    }

    // 关闭连接
    public static void close() {
        if (con != null) {
            try {
                con.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // 创建表
    public static void createTable(String tableName, String... FamilyColumn) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = getCon().getAdmin();
            HTableDescriptor htd = new HTableDescriptor(tn);
            for (String fc : FamilyColumn) {
                HColumnDescriptor hcd = new HColumnDescriptor(fc);
                htd.addFamily(hcd);
            }
            admin.createTable(htd);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 删除表
    public static void dropTable(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            Admin admin = con.getAdmin();
            admin.disableTable(tn);
            admin.deleteTable(tn);
            admin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 插入或者更新数据
    public static boolean insert(String tableName, String rowKey,
            String family, String qualifier, String value) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier),
                    Bytes.toBytes(value));
            t.put(put);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close();
        }
        return false;
    }

     public static boolean inserts(String tableName ,Put put) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));       
            t.put(put);
            put = null;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
           // HBaseUtil.close();
        }
        return false;
    }
      public static boolean inserts(String tableName ,List<Put> put) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));       
            t.put(put);
            put = null;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
           // HBaseUtil.close();
        }
        return false;
    }
    // 删除
    public static boolean del(String tableName, String rowKey, String family,
            String qualifier) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowKey));

            if (qualifier != null) {
                del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            } else if (family != null) {
                del.addFamily(Bytes.toBytes(family));
            }
            t.delete(del);
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close();
        }
        return false;
    }
    //删除一行
    public static boolean del(String tableName, String rowKey) {
        return del(tableName, rowKey, null, null);
    }
    //删除一行下的一个列族
    public static boolean del(String tableName, String rowKey, String family) {
        return del(tableName, rowKey, family, null);
    }

    // 数据读取
    //取到一个值
    public static String byGet(String tableName, String rowKey, String family,
            String qualifier) {
        try {
            Table t = getCon().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
            Result r = t.get(get);
            return Bytes.toString(CellUtil.cloneValue(r.listCells().get(0)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //取到一个族列的值
        public static Map<String, String> byGet(String tableName, String rowKey, String family) {
            Map<String, String> result = null ;
            try {
                Table t = getCon().getTable(TableName.valueOf(tableName));
                Get get = new Get(Bytes.toBytes(rowKey));
                get.addFamily(Bytes.toBytes(family));
                Result r = t.get(get);
                List<Cell> cs = r.listCells();
                result = cs.size() > 0 ? new HashMap<String, String>() : result;
                for (Cell cell : cs) {
                    result.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }
         public static Result    Getmsg(String tableName, String rowKey, String family ,Filter kof) {
         
            try {
                Table t = getCon().getTable(TableName.valueOf(tableName));
     
                Get get = new Get(Bytes.toBytes(rowKey));
                get.addFamily(Bytes.toBytes(family));
               get.setFilter(kof);//System.out.println( "weqeqw" );
                Result r = t.get(get);//System.out.println( "qqqqq" );
                System.out.println(r.size());
               
                    t.close();
                 return r;   
            } catch (IOException e) {
              System.out.println("数据查询异常"+e);        
            }
             return null;  
        }
           public static StringBuilder Scanmsg(String tableName,  String family ,String sta,String end) {
           StringBuilder result = new StringBuilder();
            try {
                Table t = getCon().getTable(TableName.valueOf(tableName));
                Scan scan = new Scan(); 
          scan.setStartRow(Bytes.toBytes(sta));
          scan.setStopRow(Bytes.toBytes(end));    
                ResultScanner  rs = t.getScanner(scan);
                rs.forEach((Result r) -> {               
                  r.listCells() .forEach((Cell cell) -> {
                      // System.out.println(Bytes.toString( CellUtil.cloneRow(cell)));
                        String start = Bytes.toString( CellUtil.cloneRow(cell)).substring(11);                
                        start = start.substring(0, 4)+"-"+start.substring(4, 6)+"-"+start.substring(6, 8)+" "+start.substring(8, 10)+":"+start.substring(10, 12)+":"+"00";
                        long time = Timestamp.valueOf(start).getTime();
                        result.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
                    });
                });
              
            
              
               
            } catch (IOException e) {
                e.printStackTrace();
            }
            return result;
        }
    //取到多个族列的值
        public static Map<String, Map<String, String>> byGet(String tableName, String rowKey) {
            Map<String, Map<String, String>> results = null ;
            try {
                Table t = getCon().getTable(TableName.valueOf(tableName));
                Get get = new Get(Bytes.toBytes(rowKey));
                Result r = t.get(get);
                List<Cell> cs = r.listCells();
                results = cs.size() > 0 ? new HashMap<String, Map<String, String>> () : results;
                for (Cell cell : cs) {
                    String familyName = Bytes.toString(CellUtil.cloneFamily(cell));
                    if (results.get(familyName) == null)
                    {
                        results.put(familyName, new HashMap<String,  String> ());
                    }
                    results.get(familyName).put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return results;
        }

  
}