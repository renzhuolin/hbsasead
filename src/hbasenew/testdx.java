/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbasenew;

import hbasenew.file.Writelog;

/**
 *
 * @author 任强
 */
public class testdx {

    long start, end;
    Hbasewrite hw = new Hbasewrite();
    String path = "/home/hadoop/earthquakedata/TestData";
    String logpath = "/home/hadoop/earthquakedata/";
    Querymsg da = new Querymsg();
    HBaseUtil HB = new HBaseUtil();
    StringBuilder result;

    public void testwrite1(String[] are) throws Exception {

        start = System.nanoTime();
        hw.sendhbase(path, 1);
        end = System.nanoTime();
        System.out.println("行键为台站测点测项采样率列为时间的写入时间: " + (end - start) / 1.0e9);
        Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率列为时间的写入时间: " + (end - start) / 1.0e9);

    }

    public void testwrite2(String[] are) throws Exception {
        start = System.nanoTime();
        hw.sendhbase(path, 2);
        end = System.nanoTime();
        System.out.println("行键为台站测点测项采样率时间的写入时间: " + (end - start) / 1.0e9);
        Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率时间的写入时间: " + (end - start) / 1.0e9);

    }

    public void testread1(String[] are) throws Exception {

        System.out.println("读取百万数据: ");
        Writelog.writeLog(logpath, "writeandreadhbaselog.log", "读取百万数据");

        da = new Querymsg();
        da.setStation("53001");
        da.setStarttime("201001010000");
        da.setEndtime("201111310000");
        da.setItem("4112");
        da.setStationpoint("2");
        da.setZoom("1");
        int i = 0;

        while (i < 20) {
            start = System.nanoTime();
            result = hbaseread.query(da);
            end = System.nanoTime();

            System.out.println("行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
            Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
            i++;
        }
        i = 1;
        while (i < 20) {
            start = System.nanoTime();
            result = hbaseread.query1(da);
            end = System.nanoTime();

            System.out.println("行键为台站测点测项采样率时间的读取时间: " + (end - start) / 1.0e9);
            Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率时间的读取时间: " + (end - start) / 1.0e9);
            i++;
        }

    }

    public void testread2(String[] are) throws Exception {
        System.out.println("读取万数据: ");
        Writelog.writeLog(logpath, "writeandreadhbaselog.log", "读取万数据");

        da.setStation("53001");
        da.setStarttime("201001010000");
        da.setEndtime("201001290000");
        da.setItem("4112");
        da.setStationpoint("2");
        da.setZoom("1");
        int i = 0;

        while (i < 20) {
            start = System.nanoTime();
            result = hbaseread.query(da);
            end = System.nanoTime();
            result.length();
            System.out.println("行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
            Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率列为时间的读取时间: " + (end - start) / 1.0e9);
            i++;
        }
        i = 1;
        while (i < 20) {
            start = System.nanoTime();
            result = hbaseread.query1(da);
            end = System.nanoTime();
            result.length();
            System.out.println("行键为台站测点测项采样率时间的读取时间: " + (end - start) / 1.0e9);
            Writelog.writeLog(logpath, "writeandreadhbaselog.log", "行键为台站测点测项采样率时间的读取时间: " + (end - start) / 1.0e9);
            i++;
        }
    }
}
