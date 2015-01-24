package com.aliyun.odps.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.aliyun.odps.Column;
import com.aliyun.odps.data.ArrayRecord;

public class MyRecord extends ArrayRecord implements Record {
    private static final Log LOG = LogFactory.getLog(MyRecord.class);
    
    public MyRecord(Column[] c) {
        super(c);
    }
    
    @Override
    public int hashCode() {
        String s = (String) this.get(0);
        return s.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        MyRecord other = (MyRecord) obj;
        return ((String) this.get(0)).equals((String) other.get(0));
    }
}

