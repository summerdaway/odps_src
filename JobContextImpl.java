package com.aliyun.odps.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.mapred.conf.JobConf;


public class JobContextImpl implements JobContext {
    private static final Log LOG = LogFactory.getLog(JobContextImpl.class);
    
    private JobConf conf = null;
    
    JobContextImpl(JobConf jc) {
        conf = jc;
    }
    
    
    public JobConf getJobConf() {
        return conf;
    }
    
    public int getNumReduceTasks() {
        return conf.getNumReduceTasks();
    }
    
    public Column[] getMapOutputKeySchema() {
        return conf.getMapOutputKeySchema();
    }
    
    public Column[] getMapOutputValueSchema() {
        return conf.getMapOutputValueSchema();
    }
    
    public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException {
        return conf.getMapperClass();
    }
    
    public Class<? extends Reducer> getCombinerClass() throws ClassNotFoundException {
        return conf.getCombinerClass();
    }
    
    public Class<? extends Reducer> getReducerClass() throws ClassNotFoundException {
        return conf.getReducerClass();
    }
    
    public String[] getGroupingColumns() {
        return null;
    }
}

