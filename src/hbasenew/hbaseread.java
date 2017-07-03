/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author 任强
 */
public class hbaseread {

    private static String tableName = "testearthauakedate";

    /*
        rowkey存非时间信息 
            行为 时间戳
     */
    public static StringBuilder query(Querymsg dbb) {
        String start = dbb.getStarttime();
        String end = dbb.getEndtime();
        String item = dbb.getItem();
        String station = dbb.getStation();
        String stationpoint = dbb.getStationpoint();
        String rate = dbb.getZoom();
        String aggregator = dbb.getAggregator();
        Timestamp sta;
        Timestamp sto;
        start = start.substring(0, 4) + "-" + start.substring(4, 6) + "-" + start.substring(6, 8) + " " + start.substring(8, 10) + ":" + start.substring(10, 12) + ":" + "00";
        sta = Timestamp.valueOf(start);
        end = end.substring(0, 4) + "-" + end.substring(4, 6) + "-" + end.substring(6, 8) + " " + end.substring(8, 10) + ":" + end.substring(10, 12) + ":" + "00";
        sto = Timestamp.valueOf(end);
        boolean minColumnInclusive = true;
        boolean maxColumnInclusive = true;
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(Long.toString(sta.getTime())), minColumnInclusive, Bytes.toBytes(Long.toString(sto.getTime())), maxColumnInclusive);
        Result r = HBaseUtil.Getmsg(getTableName(), station + stationpoint + item + rate, "date", filter);
        StringBuilder result = new StringBuilder();
        r.listCells().forEach((Cell cell) -> {
            String date = Bytes.toString(CellUtil.cloneQualifier(cell));
            Long aa = new Long(date);
            result.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(aa)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
        });
        return result;

    }

    /*
        rowkey存所有信息    
     */
    public static StringBuilder query1(Querymsg dbb) {
        String start = dbb.getStarttime();
        String end = dbb.getEndtime();
        String item = dbb.getItem();
        String station = dbb.getStation();
        String stationpoint = dbb.getStationpoint();
        String rate = dbb.getZoom();
        start = station + stationpoint + item + rate + start;
        end = station + stationpoint + item + rate + end;
        ResultScanner rs = HBaseUtil.Scanmsg("tearth", "data", start, end); 
        StringBuilder result= new StringBuilder();
          rs.forEach((Result r) -> {
                r.listCells().forEach((Cell cell) -> {
                    String ti = Bytes.toString(CellUtil.cloneRow(cell)).substring(11);
                    ti = ti.substring(0, 4) + "-" + ti.substring(4, 6) + "-" + ti.substring(6, 8) + " " + ti.substring(8, 10) + ":" + ti.substring(10, 12) + ":" + "00";
                    long time = Timestamp.valueOf(ti).getTime();
                    result.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
                });
            });
        return result;

    }
     /*
        行为 时间
     */
    public static StringBuilder query3(Querymsg dbb) {
        String start = dbb.getStarttime();
        String end = dbb.getEndtime();
        String item = dbb.getItem();
        String station = dbb.getStation();
        String stationpoint = dbb.getStationpoint();
        String rate = dbb.getZoom();
        boolean minColumnInclusive = true;
        boolean maxColumnInclusive = true;
        ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(start), minColumnInclusive, Bytes.toBytes(end), maxColumnInclusive);
        Result r = HBaseUtil.Getmsg(getTableName(), station + stationpoint + item + rate, "date", filter);
        List<Cell> cs = r.listCells();
        StringBuilder result = new StringBuilder();
          cs.forEach((Cell cell) -> {
          String date = Bytes.toString(CellUtil.cloneQualifier(cell));
          date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + " " + date.substring(8, 10) + ":" + date.substring(10, 12) + ":" + "00";
          long time = Timestamp.valueOf(date).getTime();
          result.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
        });
        return result;

    }
    
    
     public static StringBuilder query4(Querymsg dbb) {
        String start = dbb.getStarttime();
        String end = dbb.getEndtime();
        String item = dbb.getItem();
        String station = dbb.getStation();
        String stationpoint = dbb.getStationpoint();
        String rate = dbb.getZoom();
        boolean minColumnInclusive = true;
        boolean maxColumnInclusive = true;
        
       String cstart= (new Long(start.substring(0,8))+1)+"";  System.out.println(cstart);
       String cend= (new Long(end.substring(0,8)))+"";    System.out.println(cend);
       ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(start), minColumnInclusive, Bytes.toBytes(end), maxColumnInclusive);
      System.out.println(station + stationpoint + item + rate+start.substring(0,8));
       System.out.println(station + stationpoint + item + rate+end.substring(0,8));
        Result r1 = HBaseUtil.Getmsg("eardata", station + stationpoint + item + rate+start.substring(0,8), "date", filter);  
         ResultScanner r2 = null;
        if(!cend.equals(cstart))
        {
         r2 = HBaseUtil.Scanmsg("eardata", "date", station + stationpoint + item + rate+cstart,station + stationpoint + item + rate+ cend); 
        }
        Result r3 = HBaseUtil.Getmsg("eardata", station + stationpoint + item + rate+end.substring(0,8), "date", filter);
      
        
        
        
        
        StringBuilder result = new StringBuilder();
        StringBuilder result1 = new StringBuilder();
        StringBuilder result2 = new StringBuilder();
        StringBuilder result3 = new StringBuilder();
            List<Cell> cs1 = r1.listCells();
          if (cs1!=null)
          {
            cs1.forEach((Cell cell) -> {
          String date = Bytes.toString(CellUtil.cloneQualifier(cell));
          date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + " " + date.substring(8, 10) + ":" +"00:" + "00";
          long time = Timestamp.valueOf(date).getTime();
          result1.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
       
            });
          }
          
          
         if(!(r2==null))
         {
          r2.forEach((Result r) -> {
                r.listCells().forEach((Cell cell) -> {
                   String ti = Bytes.toString(CellUtil.cloneQualifier(cell));
                   // System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                    ti = ti.substring(0, 4) + "-" + ti.substring(4, 6) + "-" + ti.substring(6, 8) + " " + ti.substring(8, 10) + ":" +"00:" + "00" ;
                   long time = Timestamp.valueOf(ti).getTime();
                    result2.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
                });
            });
         }
          
           List<Cell> cs3 = r3.listCells();
             if (cs3!=null)
          {  
          cs3.forEach((Cell cell) -> {
          String date = Bytes.toString(CellUtil.cloneQualifier(cell));
          date = date.substring(0, 4) + "-" + date.substring(4, 6) + "-" + date.substring(6, 8) + " " + date.substring(8, 10) + ":"+"00:" + "00";
          long time = Timestamp.valueOf(date).getTime();
          result3.append((new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(time)).append(",").append(Bytes.toString(CellUtil.cloneValue(cell))).append("\n");
        }); 
          }
            
                 
             result=result1.append(result2).append(result3);
        return result;
         
    }

    /**
     * @return the tableName
     */
    public static String getTableName() {
        return tableName;
    }

    /**
     * @param aTableName the tableName to set
     */
    public static void setTableName(String aTableName) {
        tableName = aTableName;
    }

}
