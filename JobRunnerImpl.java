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

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.FileNotFoundException;


public class JobRunnerImpl extends Configured implements JobRunner {
    private static final Log LOG = LogFactory.getLog(JobRunnerImpl.class);
    
    JobRunnerImpl() {
        
    }
    
    public RunningJob submit() throws OdpsException {
        JobConf conf = (JobConf) this.getConf();
        LOG.info(conf.getMapperClass());
        LOG.info(conf.getReducerClass());
        LOG.info(conf.getMemoryForMapTask());
        LOG.info(conf.getNumMapTasks());
        LOG.info(conf.getNumReduceTasks());
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
        return null;
    }
}

