package com.aliyun.odps.mapred;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.OdpsType;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.io.BufferedWriter;
import java.io.FileWriter;

import java.lang.instrument.Instrumentation;

public class MapperTaskContextImpl extends TaskContextImpl implements Mapper.TaskContext {
    private static final Log LOG = LogFactory.getLog(MapperTaskContextImpl.class);
    private static int maxNum = 50000;
    public static int numMapOutput = 0;
    public static HashMap<Record, List<Record>> mapOutputRecords = new HashMap<Record, List<Record>>();
    MapperTaskContextImpl(JobConf jc) {
        super(jc);
    }

    public long getCurrentRecordNum() {
        return 0;
    }
    
    public Record getCurrentRecord() {
        return null;
    }
    
    public boolean nextRecord() {
        return false;
    }
    
    public TableInfo getInputTableInfo() {
        return null;
    }
    
    public void combineRecords() {
        
    }
    
    public void output() {
        numMapOutput++;
        List<Map.Entry<Record, List<Record>>> lt = new ArrayList<Map.Entry<Record, List<Record>>>(mapOutputRecords.entrySet());
        mapOutputRecords.clear();
        Collections.sort(lt, new Comparator<Map.Entry<Record, List<Record>>>() {
            public int compare(Map.Entry<Record, List<Record>> x, Map.Entry<Record, List<Record>> y) {
                return ((String) x.getKey().get(0)).compareTo((String) y.getKey().get(0));
            }
        });
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter("output_"+numMapOutput));
            int len = lt.size();
            for(int i = 0; i < len; ++i) {
                Map.Entry<Record, List<Record>> now = lt.get(i);
                List<Record> lr = now.getValue();
                int len1 = lr.size();
                for(int j = 0; j < len1; ++j) {
                    bw.write(now.getKey().get(0)+","+lr.get(j).get(0)+"\n");
                }
            }
            bw.flush();
            
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    public void write(Record key, Record value) throws IOException {
        //LOG.info(key.get(0) + " "+value.get(0));
        Record newKey = new ArrayRecord(key.getColumns());
        newKey.set(key.toArray());
        if(!mapOutputRecords.containsKey(newKey)) {
            // calculate size
            ArrayList<Record> al = new ArrayList<Record>();
            al.add(value);
            mapOutputRecords.put(newKey, al);
        } else {
            LOG.info("has key");
            ArrayList<Record> al = (ArrayList<Record>) mapOutputRecords.get(newKey);
            al.add(value);
        }
        if(mapOutputRecords.size() == maxNum) {// call Combiner
            combineRecords();
        }
        long freeMemory = Runtime.getRuntime().freeMemory();
        long totalMemory = Runtime.getRuntime().maxMemory();
        if(freeMemory < totalMemory*0.7 && mapOutputRecords.size() > (maxNum>>1)) {
            output();
        }
    }
}

