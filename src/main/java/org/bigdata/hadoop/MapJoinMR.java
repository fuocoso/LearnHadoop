package org.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/7/10.
 * customer.txt
 * 1,Stephanie Leung,555-555-5555
 * 2,Edward Kim,123-456-7890
 * 3,Jose Madriz,281-330-8004
 * 4,David Stork,408-555-0000
 * <p>
 * order.txt
 * 3,A,12.95,02-Jun-2008
 * 1,B,88.25,20-May-2008
 * 2,C,32.00,30-Nov-2007
 * 3,D,25.02,22-Jan-2009
 */

/**
 * 使用场景：一张表十分小、一张表很大。
 用法:在提交作业的时候先将小表文件放到该作业的DistributedCache中，
 然后从DistributeCache中取出该小表进行join key / value解析分割放到内存中（可以放大Hash Map等等容器中）。
 然后扫描大表，看大表中的每条记录的join key /value值是否能够在内存中找到相同join key的记录，如果有则直接输出结果。
 */
public class MapJoinMR extends Configured implements Tool {
    //在分布式缓冲中存放的表
    private static  String cacaeFile  = "hdfs://com.hadoop05:8020/data/customer.txt";
    public static class CustomData implements Writable{
            private int customID;
            private String name;
            private String phome;

        public CustomData() {
        }

        public CustomData(int customID, String name, String phome) {
            this.customID = customID;
            this.name = name;
            this.phome = phome;
        }

        public void set(int customID, String name, String phome){
            this.customID = customID;
            this.name = name;
            this.phome = phome;
        }

        public int getCustomID() {
            return customID;
        }

        public void setCustomID(int customID) {
            this.customID = customID;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getPhome() {
            return phome;
        }

        public void setPhome(String phome) {
            this.phome = phome;
        }

        public void write(DataOutput out) throws IOException {

        }

        public void readFields(DataInput in) throws IOException {

        }

        @Override
        public String toString() {
            return customID +","+
                     name +","+ phome;
        }
    }
    public static class MapJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


        private Map<Integer,CustomData> costomMap = new HashMap<Integer, CustomData>();
        private IntWritable outPutKey = new IntWritable();
        private Text outPutValue = new Text();


        /**
         *此方法在每个task开始之前执行，这里主要用作从DistributedCache   
         * 中取到costomer文件，并将里边记录取出放到内存中。   
         */

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            //获取当前作业的DistributedCache相关文件
            //Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            FileSystem fs = FileSystem.get(URI.create(cacaeFile),context.getConfiguration());
            FSDataInputStream fdis  = fs.open(new Path(cacaeFile));
            BufferedReader br = new BufferedReader(new InputStreamReader(fdis));

            String lines = null;
            String[] clos = null;

            while ((lines=br.readLine())!=null){
                clos = lines.split(",");
                if (clos.length<3){
                    continue;
                }
                CustomData customData = new CustomData(Integer.parseInt(clos[0]),clos[1],clos[2]);
                System.err.println(customData.getCustomID());
                costomMap.put(customData.getCustomID(),customData);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] strs = value.toString().split(",");

            //格式：costom_id,order_id,prices,order_date
            if (strs.length<4){
                return;
            }
          int joinID = Integer.valueOf(strs[0]);
            System.err.println(joinID);
            CustomData costomjd = costomMap.get(joinID);


            //如果内存中没有的用户信息没有可以join的数据，则过滤
            if (costomjd == null){
                return;
            }
            StringBuffer sbf = new StringBuffer();
            sbf.append(strs[1]).append(",")
                    .append(strs[2]).append(",")
                    .append(strs[3]).append(",")
            .append(costomjd.getName()).append(",")
            .append(costomjd.getPhome());


            outPutKey.set(joinID);
            outPutValue.set(sbf.toString());
            context.write(outPutKey,outPutValue);

        }
    }

public static class  MapJoinReducer extends Reducer<IntWritable,Text,IntWritable,Text>{
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values){
            context.write(key,value);
        }
    }
}

    public int run(String[] args) throws Exception {
        //1.获取hadoop的配置信息
        Configuration config = super.getConf();
        //config.set();

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
        job.addCacheFile(URI.create(cacaeFile));
        FileInputFormat.addInputPath(job,inPath);


        //3.2:map
        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        /**
         * shuffle 过程
         * 按照map输出的key进行分区和分组
         * 默认 PairWritable
         * 自定义分区 new PairWritable().getName()
         * 自定义分组 new PairWritable().getName()
         */
        //job.setPartitionerClass(MyPartition.class);
        //job.setGroupingComparatorClass(MyGroup.class);

        //3.reducer
        //job.setReducerClass(MapJoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
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
                new MapJoinMR(), args);
        System.exit(status);
    }
}
