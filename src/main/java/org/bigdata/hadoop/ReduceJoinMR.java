package org.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
 * 在Reudce端进行连接是MapReduce框架进行表之间join操作最为常见的模式，其具体的实现原理如下：
 Map端的主要工作：为来自不同表（文件）的key/value对打标签以区别不同来源的记录。然后用连接字段作为key，
 其余部分和新加的标志作为value，最后进行输出。
 reduce端的主要工作：在reduce端以连接字段作为key的分组已经完成，
 我们只需要在每一个分组当中将那些来源于不同文件的记录（在map阶段已经打标志）分开，最后进行笛卡尔只就ok了。
 */
public class ReduceJoinMR extends Configured implements Tool {
    public static class ReducerJoinMapper extends Mapper<LongWritable, Text, Text, JoinData> {
        private Text mapOutKey = new Text();
        private JoinData mapOutValue = new JoinData();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取输入文件路径
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (pathName.contains("customer")) {
                String line = value.toString();
                String[] strs = line.split(",");
                mapOutKey.set(strs[0]);
                mapOutValue.set(strs[0], "0", strs[1] + "," + strs[2]);
                context.write(mapOutKey, mapOutValue);
            } else if (pathName.contains("order")) {
                String line = value.toString();
                String[] strs = line.split(",");
                mapOutKey.set(strs[0]);
                mapOutValue.set(strs[0], "1", strs[1] + "," + strs[2] + "," + strs[3]);
                context.write(mapOutKey, mapOutValue);
            }
        }
    }

    public static class ReducerJoinReducer extends Reducer<Text, JoinData, Text, Text> {
        private List<String> leftTable = new ArrayList<String>();
        private List<String> rightTable = new ArrayList<String>();
        private  Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {
            leftTable.clear();
            rightTable.clear();
            for (JoinData bean : values) {
                String flag = bean.getFlag();
                //
                if (flag.equals("0")) {
                    leftTable.add(bean.getSecondPart());
                } else if (flag.equals("1")) {
                    rightTable.add(bean.getSecondPart());
                }
            }

            for (String lt : leftTable) {
                for (String rt:rightTable ) {
                    outValue.set(lt+","+rt);
                    context.write(key,outValue);
                }
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
        FileInputFormat.setInputPaths(job, inPath);


        //3.2:map
        job.setMapperClass(ReducerJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinData.class);

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
        job.setReducerClass(ReducerJoinReducer.class);
        job.setOutputKeyClass(Text.class);
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
//        args = new String[]{
//                "hdfs://ns1/input/secondary.txt",
//                "hdfs://ns1/output",
//                "E:\\mapReduce\\join",
//                "E:\\mapReduce\\output"
//        };

        int status = ToolRunner.run(conf,
                new ReduceJoinMR(), args);
        System.exit(status);
    }
}
