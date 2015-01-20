package com.aliyun.odps.mapred;

import com.aliyun.odps.Column;
import com.aliyun.odps.mapred.conf.JobConf;


public class JobContextImpl implements JobContext {
    private static final Log LOG = LogFactory.getLog(JobContextImpl.class);
    
    private JobConf conf = null;
    
    JobContextImpl(JobConf jc) {
        conf = jc;
    }
    
    @override
    public JobConf getJobConf() {
        return conf;
    }
    
    @override
    public int getNumReduceTasks() {
        return conf.getNumReduceTasks();
    }
    
    @override
    public Column[] getMapOutputKeySchema() {
        return conf.getMapOutputKeySchema();
    }
    
    @override
    public Column[] getMapOutputValueSchema() {
        return conf.getMapOutputValueSchema();
    }
    
    @override
    public Class<? extends Mapper> getMapperClass() throws ClassNotFoundException {
        return conf.getMapperClass();
    }
    
    @override
    public Class<? extends Reducer> getCombinerClass() throws ClassNotFoundException {
        return conf.getCombinerClass();
    }
    
    @override
    public Class<? extends Reducer> getReducerClass() throws ClassNotFoundException {
        return conf.getReducerClass();
    }
    
    @override
    String[] getGroupingColumns() {
        return null;
    }
}

