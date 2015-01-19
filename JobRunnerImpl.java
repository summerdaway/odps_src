package com.aliyun.odps.mapred;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.conf.Configurable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.conf.Configured;


public class JobRunnerImpl extends Configured implements JobRunner {
    private static final Log LOG = LogFactory.getLog(JobRunnerImpl.class);
    
    JobRunnerImpl() {
        
    }
    
    public RunningJob submit() throws OdpsException {
        LOG.info("aaaaa");
        JobConf conf = (JobConf) this.getConf();
        System.out.println(conf.getMapperClass());
        return null;
    }
}

