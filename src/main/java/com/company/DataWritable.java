package com.company;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

public class DataWritable implements Writable {

    private String loginIP;
    private long data;
    public DataWritable(){}
    public DataWritable(String loginIP, long data) {
        this.loginIP = loginIP;
        this.data = data;
    }

    public String getLogin() {
        return loginIP;
    }
    public long getData() {
        return data;
    }
    public void setLogin(String loginIP) {
        this.loginIP = loginIP;
    }
    public void setData(long data) {
        this.data = data;
    }

    public void readFields(DataInput in) throws IOException {
        this.loginIP = in.readUTF();
        this.data = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.loginIP);
        out.writeLong(this.data);
    }
    @Override
    public String toString(){
        return  loginIP + "\\" + new Date(data) ;
    }

}