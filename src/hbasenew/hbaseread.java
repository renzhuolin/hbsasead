/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import java.sql.Timestamp;
import java.util.Map;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnRangeFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 *
 * @author 任强
 */
public class hbaseread {
    private static   String tableName = "testearthauakedate";
     /*
        rowkey存非时间信息    
    */
    public static StringBuilder query(Querymsg dbb){
                String start = dbb.getStarttime();
                String end = dbb.getEndtime();
                String item = dbb.getItem();
                String station = dbb.getStation();
                String stationpoint = dbb.getStationpoint();
                String rate = dbb.getZoom();
                String aggregator = dbb.getAggregator();
       Timestamp sta ;
         Timestamp sto ;
         start = start.substring(0, 4)+"-"+start.substring(4, 6)+"-"+start.substring(6, 8)+" "+start.substring(8, 10)+":"+start.substring(10, 12)+":"+"00";
      sta = Timestamp.valueOf(start);
       end = end.substring(0, 4)+"-"+end.substring(4, 6)+"-"+end.substring(6, 8)+" "+end.substring(8, 10)+":"+end.substring(10, 12)+":"+"00";
      sto = Timestamp.valueOf(end);
   boolean minColumnInclusive = true;
boolean maxColumnInclusive = true;
ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(Long.toString(sta.getTime())), minColumnInclusive, Bytes.toBytes(Long.toString(sto.getTime())), maxColumnInclusive);
    //ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("1325347200000"), minColumnInclusive, Bytes.toBytes("1383148800000"), maxColumnInclusive);
        //Map<String, String> result = HBaseUtil.Getmsg(getTableName(), station+stationpoint+item+rate, "date",filter);
         StringBuilder result = HBaseUtil.Getmsg(getTableName(), station+stationpoint+item+rate, "date",filter);
    return result;
  
    }
    /*
        rowkey存所有信息    
    */
     public static StringBuilder query1(Querymsg dbb){
                String start = dbb.getStarttime();
                String end = dbb.getEndtime();
                String item = dbb.getItem();
                String station = dbb.getStation();
                String stationpoint = dbb.getStationpoint();
                String rate = dbb.getZoom();
                String aggregator = dbb.getAggregator();
               start=   station+stationpoint+item+rate+start;
                end=   station+stationpoint+item+rate+end;
         StringBuilder result =  HBaseUtil.Scanmsg("tearth","data", start, end);
   
    return result;
  
    }
      public static StringBuilder query3(Querymsg dbb){
                String start = dbb.getStarttime();
                String end = dbb.getEndtime();
                String item = dbb.getItem();
                String station = dbb.getStation();
                String stationpoint = dbb.getStationpoint();
                String rate = dbb.getZoom();
                String aggregator = dbb.getAggregator();
      
      
    
   boolean minColumnInclusive = true;
boolean maxColumnInclusive = true;
ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes(start), minColumnInclusive, Bytes.toBytes(end), maxColumnInclusive);
    //ColumnRangeFilter filter = new ColumnRangeFilter(Bytes.toBytes("1325347200000"), minColumnInclusive, Bytes.toBytes("1383148800000"), maxColumnInclusive);
        //Map<String, String> result = HBaseUtil.Getmsg(getTableName(), station+stationpoint+item+rate, "date",filter);
         StringBuilder result = HBaseUtil.Getmsg(getTableName(), station+stationpoint+item+rate, "date",filter);
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
