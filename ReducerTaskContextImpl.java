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

public class ReducerTaskContextImpl extends TaskContextImpl implements Reducer.TaskContext {
    private static final Log LOG = LogFactory.getLog(ReducerTaskContextImpl.class);
    public static BufferedWriter bw = null;
    
    
    ReducerTaskContextImpl(JobConf jc) {
        super(jc);
        try {
            bw = new BufferedWriter(new FileWriter("ans"));
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    public boolean nextKeyValue() {
        return false;
    }
    
    public Record getCurrentKey() {
        return null;
    }
    
    public Iterator<Record> getValues() {
        return null;
    }
    
    public void write(Record r) throws IOException {
        LOG.info(r.get(0)+","+r.get(1));
        bw.write(r.get(0)+","+r.get(1)+"\n");
        bw.flush();
    }
    
    
    public void write(Record key, Record value) throws IOException {
        //LOG.info(key.get(0) + " "+value.get(0));
        List<Record> val = (List<Record>) MapperTaskContextImpl.mapOutputRecords.get(key);
        val.clear();
        ArrayRecord ar = new ArrayRecord(value.getColumns());
        ar.set(value.toArray());
        val.add(ar);
        LOG.info(val);
    }
}

