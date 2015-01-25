package com.aliyun.odps.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.io.BufferedInputStream;
import java.util.Iterator;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.TableInfo;
import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.counter.Counter;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.OdpsType;

public class TaskContextImpl extends JobContextImpl implements TaskContext {
    private static final Log LOG = LogFactory.getLog(TaskContextImpl.class);
    
    TaskContextImpl(JobConf jc) {
        super(jc);
    }
    
    public TaskId getTaskID() {
        return null;
    }
    
    public TableInfo[] getOutputTableInfo() throws IOException {
        return null;
    }
    
    public Record createOutputRecord() throws IOException {
        return new ArrayRecord(new Column[]{new Column("word", OdpsType.STRING), new Column("word", OdpsType.BIGINT)});
    }
    
    public Record createOutputRecord(String label) throws IOException {
        return null;
    }
    
    public Record createOutputKeyRecord() throws IOException {
        return null;
    }
    
    public Record createOutputValueRecord() throws IOException {
        return null;
    }
    
    public Record createMapOutputKeyRecord() throws IOException {
        return new ArrayRecord(conf.getMapOutputKeySchema());
    }
    
    public Record createMapOutputValueRecord() throws IOException {
        Column[] c = conf.getMapOutputValueSchema();
        return new ArrayRecord(conf.getMapOutputValueSchema());
    }
    
    public BufferedInputStream readResourceFileAsStream(String resourceName) throws IOException {
        return null;
    }
    
    public Iterator<Record> readResourceTable(String resourceName) throws IOException {
        return null;
    }
    
    public Counter getCounter(Enum<?> name) {
        return null;
    }
    
    public Counter getCounter(String group, String name) {
        return null;
    }
    
    public void progress() {
        
    }
    
    public void write(Record record) throws IOException {
        
    }
    
    public void write(Record record, String label) throws IOException {
        
    }
    
    public void write(Record key, Record value) throws IOException {
        
    }
}

