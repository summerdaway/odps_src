package com.aliyun.odps.mapred;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.mapred.conf.JobConf;
import com.aliyun.odps.counter.Counters;
import com.aliyun.odps.mapred.JobStatus;

import java.lang.Thread;
import java.io.IOException;


public class RunningJobImpl implements RunningJob {
    private static final Log LOG = LogFactory.getLog(RunningJob.class);
    
    private JobConf conf;
    
    RunningJobImpl(JobConf cf) {
        conf = cf;
    }
    
    public String getInstanceID() {
        return "job-0001";
    }
    
    public boolean isComplete() {
        return true;
    }
    
    public boolean isSuccessful() {
        return true;
    }
    
    public void waitForCompletion() {
        while(!isComplete()) {
            try {
                Thread.sleep(5000);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        if(isSuccessful()) {
            LOG.info("job success");
        } else {
            LOG.info("job fail");
        }
    }
    
    public JobStatus getJobStatus() {
        return JobStatus.SUCCEEDED;
    }
    
    public void killJob() {
        
    }
    
    public Counters getCounters() {
        return new Counters();
    }
    
    public String getDiagnostics() {
        return "";
    }
    
    public float mapProgress() throws IOException {
        return 1.0f;
    }
    
    public float reduceProgress() throws IOException {
        return 1.0f;
    }
}

