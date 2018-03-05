package org.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/8/27.
 */
public class Join extends Configured implements Tool {
    private static final String CUSTOMER_CACHE_URI = "hdfs://apache.bigdata.com:8020/input/mj/custom";
    public static class CustomBean implements Writable {
        private int custID;
        private String name;
        private String address;
        private String phone;

        public CustomBean() {
        }

        public CustomBean(int custID, String name, String address, String phone) {
            this.custID = custID;
            this.name = name;
            this.address = address;
            this.phone = phone;
        }

        public void set(int custID, String name, String address, String phone) {
            this.custID = custID;
            this.name = name;
            this.address = address;
            this.phone = phone;
        }

        public int getCustID() {
            return custID;
        }

        public void setCustID(int custID) {
            this.custID = custID;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPhone() {
            return phone;
        }

        public void setPhone(String phone) {
            this.phone = phone;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public void write(DataOutput out) throws IOException {
                out.writeInt(custID);
                out.writeUTF(name);
                out.writeUTF(address);
                out.writeUTF(phone);
        }

        public void readFields(DataInput in) throws IOException {
            custID = in.readInt();
            name = in.readUTF();
            address = in.readUTF();
            phone = in.readUTF();
        }

        @Override
        public String toString() {
            return custID +
                    "," + name +
                    "," + address +
                    "," + phone;
        }
    }

    public static class CustOrderMapOutKey implements WritableComparable<CustOrderMapOutKey> {
        private int custID;
        private int orderID;

        public CustOrderMapOutKey() {
        }

        public CustOrderMapOutKey(int custID, int orderID) {
            this.custID = custID;
            this.orderID = orderID;
        }

        public void set(int custID,int orderID){
            this.custID = custID;
            this.orderID = orderID;
        }

        public int getCustID() {
            return custID;
        }

        public void setCustID(int custID) {
            this.custID = custID;
        }

        public int getOrderID() {
            return orderID;
        }

        public void setOrderID(int orderID) {
            this.orderID = orderID;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CustOrderMapOutKey that = (CustOrderMapOutKey) o;

            if (custID != that.custID) return false;
            return orderID == that.orderID;
        }

        @Override
        public int hashCode() {
            int result = custID;
            result = 31 * result + orderID;
            return result;
        }

        public int compareTo(CustOrderMapOutKey o) {
            int res = Integer.valueOf(custID).compareTo(Integer.valueOf(o.getCustID()));
            return res = (res ==0) ? Integer.valueOf(orderID).compareTo(Integer.valueOf(o.getOrderID())):res;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(custID);
            out.writeInt(orderID);
        }

        public void readFields(DataInput in) throws IOException {
            custID = in.readInt();
            orderID = in.readInt();
        }

        @Override
        public String toString() {
            return  custID +
                    "," + orderID;
        }
    }

    public static class JoinMapper extends Mapper<LongWritable, Text, CustOrderMapOutKey, Text> {
        private CustOrderMapOutKey outputKey = new CustOrderMapOutKey();
        private Text outputVlaue = new Text();

        /**
         * 在内存中customer数据
         */
        private static final Map<Integer,CustomBean> CUSTOMER_MAP = new HashMap<Integer, CustomBean>();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 格式: 订单编号 客户编号    订单金额
            String[] strs = value.toString().split(",");
            if(strs.length<3){
                return;
            }
            int customID = Integer.valueOf(strs[1]);
            CustomBean customBean = CUSTOMER_MAP.get(customID);
            if (customBean == null){
                return;
            }
            StringBuffer sb = new StringBuffer();
            sb.append(strs[2]).append(",")
                    .append(customBean.getName()).append(",")
                    .append(customBean.getAddress()).append(",")
                    .append(customBean.getPhone());
            outputKey.set(customID,Integer.parseInt(strs[0]));
            outputVlaue.set(sb.toString());
            context.write(outputKey,outputVlaue);
        }
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(URI.create(CUSTOMER_CACHE_URI),context.getConfiguration());
            FSDataInputStream fdis = fs.open(new Path(CUSTOMER_CACHE_URI));

            BufferedReader reader = new BufferedReader(new InputStreamReader(fdis));
            String line = null;
            String[] cols = null;
            // 格式：客户编号  姓名  地址  电话
            while ((line = reader.readLine())!=null){
                cols = line.split(",");
                // 数据格式不匹配，忽略
                if(cols.length<4){
                    continue;
                }
                CustomBean bean = new CustomBean(Integer.parseInt(cols[0]),cols[1],cols[2],cols[3]);
                CUSTOMER_MAP.put(bean.getCustID(),bean);
            }

        }
    }

    public static class JoinReduce extends Reducer<CustOrderMapOutKey, Text, CustOrderMapOutKey, Text> {
        @Override
        protected void reduce(CustOrderMapOutKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                for (Text value : values){
                    context.write(key,value);
                }
        }
    }

    public int run(String[] args) throws Exception {
        //1.获取hadoop的配置信息
        Configuration config = new Configuration();
        //2.生成对应的job
        Job job = Job.getInstance(
                config,
                this.getClass().getSimpleName()
        );
        job.setJarByClass(getClass());

        //3:设置job内容
        //input  -> map ->reduce -> output
        //3.1 输入
        Path inPath = new Path(args[0]);
        job.addCacheFile(URI.create(CUSTOMER_CACHE_URI));
        FileInputFormat.addInputPath(job,inPath);

        //3.2:map
        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(CustOrderMapOutKey.class);
        job.setMapOutputValueClass(Text.class);

        //3.reducer
        job.setReducerClass(JoinReduce.class);
        job.setOutputKeyClass(CustOrderMapOutKey.class);
        job.setOutputValueClass(Text.class);
        //3.4输出
        Path outPath = new Path(args[1]);
        //输出目录如果存在自动删除

        FileSystem fs = outPath.getFileSystem(config);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        FileOutputFormat.setOutputPath(job, outPath);

        //4.提交的job运行是否成功
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int status = ToolRunner.run(conf,
                new Join(), args);
        System.exit(status);
    }
}
