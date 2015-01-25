package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configurable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.conf.Configured;
import com.aliyun.odps.mapred.utils.InputUtils;
import com.aliyun.odps.mapred.utils.OutputUtils;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.utils.ReflectionUtils;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.Column;
import com.aliyun.odps.OdpsType;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;


public class JobRunnerImpl extends Configured implements JobRunner {
    private static final Log LOG = LogFactory.getLog(JobRunnerImpl.class);
    
    JobRunnerImpl() {
        
    }
    
    public Record toRecord(String s) {
        Column[] c = new Column[1];
        c[0] = new Column("word", OdpsType.STRING);
        ArrayRecord ret = new ArrayRecord(c);
        ret.set(0, s);
        return ret;
    }
    
    public Record toRecord(Long s) {
        Column[] c = new Column[1];
        c[0] = new Column("word", OdpsType.BIGINT);
        ArrayRecord ret = new ArrayRecord(c);
        ret.set(0, s);
        return ret;
    }
    
    public RunningJob submit() throws OdpsException {
        JobConf conf = (JobConf) this.getConf();
        LOG.info("Job start");
        Mapper.TaskContext mapperContext = (Mapper.TaskContext) new MapperTaskContextImpl(conf);
        Class mapper = conf.getMapperClass();
        Mapper mapperInstance = null;
        try {
            LOG.info("Map setup");
            mapperInstance = (Mapper) mapper.newInstance();
            Method mapperSetup = mapper.getMethod("setup", Mapper.TaskContext.class);
            mapperSetup.invoke(mapperInstance, mapperContext);
            
            TableInfo[] inputTable = InputUtils.getTables(conf);
            BufferedReader bf = null;
            bf = new BufferedReader(new FileReader(inputTable[0].toString()));
            Method mapperMap = mapper.getMethod("map", long.class, Record.class, Mapper.TaskContext.class);
            long recordNum = 0;
            String buf;
            LOG.info("Map 0%");
            long totalLength = (new File(inputTable[0].toString())).length();
            long nowLength = 0;
            long beforeLength = 0;
            while((buf = bf.readLine()) != null) {
                String[] s = buf.split(",");
                ++recordNum;
                int len = s.length;
                Column[] c = new Column[len];
                for(int i = 0; i < len; ++i) {
                    c[i] = new Column("word", OdpsType.STRING);
                }
                ArrayRecord ar = new ArrayRecord(c);
                ar.set(s);
                mapperMap.invoke(mapperInstance, recordNum, ar, mapperContext);
                nowLength += buf.length();
                if((nowLength - beforeLength) >= 0.05*totalLength) {
                    beforeLength = nowLength;
                    LOG.info("Map " + (100.0*nowLength/totalLength) + "%");
                }
            }
            ((MapperTaskContextImpl)mapperContext).combineRecords();
            ((MapperTaskContextImpl)mapperContext).output();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        LOG.info("Shuffle start");
        // Reduce
        String[] fileNames = new String[MapperTaskContextImpl.numMapOutput];
        for(int i = 0; i < MapperTaskContextImpl.numMapOutput; ++i) {
            fileNames[i] = "output_" + (i+1);
        }
        FileMerge fm = new FileMerge();
        try {
            fm.fileMerge(fileNames);
        } catch(Exception e) {
            e.printStackTrace();
        }
        LOG.info("Reduce start");
        Class reducer = conf.getReducerClass();
        Reducer.TaskContext reducerContext = (Reducer.TaskContext) new ReducerTaskContextImpl(conf);
        try {
            Reducer reducerInstance = (Reducer) reducer.newInstance();
            Method reducerSetup = reducer.getMethod("setup", Reducer.TaskContext.class);
            reducerSetup.invoke(reducerInstance, reducerContext);
            
            Method reduce = reducer.getMethod("reduce", Record.class, Iterator.class, Reducer.TaskContext.class);
            BufferedReader bf = new BufferedReader(new FileReader("reduceInput"));
            String currentLine = null;
            String key = null;
            List<Record> ri = new ArrayList<Record>();
            while((currentLine = bf.readLine()) != null) {
                String[] s = currentLine.split(",");
                if(!s[0].equals(key)) {
                    if(key != null) {
                        Iterator<Record> iter = ri.iterator();
                        reduce.invoke(reducerInstance, toRecord(key), iter, reducerContext);
                    }
                    ri.clear();
                    key = s[0];
                }
                ri.add(toRecord(Long.parseLong(s[1])));
            }
            reduce.invoke(reducerInstance, toRecord(key), ri.iterator(), reducerContext);
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        
        /*
        String[] s = conf.getOutputKeySortColumns();
        LOG.info(s.length);
        for(int i = 0; i < s.length; ++i) {
            LOG.info(s[i]);
        }
        LOG.info(conf.getInstancePriority());
        TableInfo[] inputTable = InputUtils.getTables(conf);
        TableInfo[] outputTable = OutputUtils.getTables(conf);
        LOG.info(inputTable[0].getTableName());
        LOG.info(outputTable[0]);
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(inputTable[0].toString()));
        } catch(Exception e) {
            LOG.info(e);
        }
        String buf;
        HashMap<String, Integer> map = new HashMap<String, Integer>();
        try {
            while((buf = bf.readLine()) != null) {
                s = buf.split(",");
                for(int i = 0; i < 2; ++i) {
                    if(!map.containsKey(s[i])) {
                        map.put(s[i], 1);
                    } else {
                        map.put(s[i], map.get(s[i])+1);
                    }
                }
            }
        } catch(Exception e){}
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(outputTable[0].toString()));
        } catch(Exception e){
            LOG.info(e);
        }
        Iterator iter = map.entrySet().iterator();
        while(iter.hasNext()) {
            Entry entry = (Entry) iter.next();
            String key = (String)entry.getKey();
            int value = ((Integer)entry.getValue()).intValue();
            try {
                //LOG.info(key + "," + value);
                bw.write(key + "," + value + "\n");
            } catch(Exception e){LOG.info(e);}
        }
        try {
            bw.flush();
        } catch(Exception e) {}
        //System.gc();
        long total = Runtime.getRuntime().maxMemory();
        long m1 = Runtime.getRuntime().freeMemory();
        LOG.info("total: " + total + " free: " + m1);
         */
        return new RunningJobImpl(conf);
    }
}

