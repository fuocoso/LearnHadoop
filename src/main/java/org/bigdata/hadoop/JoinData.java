package org.bigdata.hadoop;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Administrator on 2017/7/10.
 */
public class JoinData implements Writable {
    //用来join的key
    private String joinkey;
    //用来标识数据来源的哪张表
    private String flag;
    //数据的join外的字段
    private String secondPart;

    public JoinData() {
    }

    public JoinData(String joinkey, String flag, String secondPart) {
        this.joinkey = joinkey;
        this.flag = flag;
        this.secondPart = secondPart;
    }

    public void set(String joinkey, String flag, String secondPart) {
        this.joinkey = joinkey;
        this.flag = flag;
        this.secondPart = secondPart;

    }

    public String getJoinkey() {
        return joinkey;
    }

    public void setJoinkey(String joinkey) {
        this.joinkey = joinkey;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public String getSecondPart() {
        return secondPart;
    }

    public void setSecondPart(String secondPart) {
        this.secondPart = secondPart;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(joinkey);
        out.writeUTF(flag);
        out.writeUTF(secondPart);
    }

    public void readFields(DataInput in) throws IOException {
        this.joinkey = in.readUTF();
        this.flag = in.readUTF();
        this.secondPart = in.readUTF();
    }

    @Override
    public String toString() {
        return joinkey + ";" + flag + ";" +secondPart;
    }
}
