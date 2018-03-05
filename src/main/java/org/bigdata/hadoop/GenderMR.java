package org.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @ProjectName BestScoreCount
 * @PackageName com.buaa
 * @ClassName Gender *
 * @Description 统计不同年龄段内，男、女最高分数
 * @Author Administrator
 * @Date 2017-07-31 21:35:50
 */

public class GenderMR extends Configured implements Tool {
    private static String TAB_SEPARATOR = "\t";

    public static class GenderMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * 调用map解析一行数据，该行的数据存储在value参数中，然后根据\t分隔符，
         * 解析出姓名，年龄，性别和成绩
         */
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /**
             * 姓名 年龄 性别 成绩
             * Alice 23 female 45 * 每个字段的分隔符是tab键
             */

            // 使用\t,分割数据
            String[] tokens = value.toString().split(TAB_SEPARATOR);
            // 性别
            String gender = tokens[2];
            // 姓名 年龄 成绩
            String nameAgeScore = tokens[0] + TAB_SEPARATOR + tokens[1] + TAB_SEPARATOR + tokens[3];
            // 输出key=gender value=name+age+score
            context.write(new Text(gender), new Text(nameAgeScore));
        }
    }


/**
 * 合并 Mapper输出结果
 */
public static class GenderCombiner extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxScore = Integer.MIN_VALUE;
        int score = 0;
        String name = " ";
        String age = " ";
        for (Text val : values) {
            String[] valTokens = val.toString().split(TAB_SEPARATOR);
            score = Integer.parseInt(valTokens[2]);
            if (score > maxScore) {
                name = valTokens[0];
                age = valTokens[1];
                maxScore = score;
            }
        }
        context.write(key, new Text(name + TAB_SEPARATOR + age + TAB_SEPARATOR + maxScore));
    }
}

/**
 * 根据 age年龄段将map输出结果均匀分布在reduce上
 */
public static class GenderPartitioner extends Partitioner<Text, Text> {
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String[] nameAgeScore = value.toString().split(TAB_SEPARATOR);
        // 学生年龄
        int age = Integer.parseInt(nameAgeScore[1]);
        // 默认指定分区 0
        if (numReduceTasks == 0) {
            return 0;
        }
        // 年龄小于等于20，指定分区0
        if (age <= 20) {
            return 0;
        } else if (age <= 50) {
            // 年龄大于20，小于等于50，指定分区1
            return 1 % numReduceTasks;
        } else {
            // 剩余年龄，指定分区2
            return 2 % numReduceTasks;
        }
    }
}
    /* * 统计出不同性别的最高分 */
    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxScore = Integer.MIN_VALUE;
            int score = 0;
            String name = " ";
            String age = " ";
            String gender = " ";
            // 根据key，迭代 values集合，求出最高分
            for (Text val : values) {
                String[] valTokens = val.toString().split(TAB_SEPARATOR);
                score = Integer.parseInt(valTokens[2]);
                if (score > maxScore) {
                    name = valTokens[0];
                    age = valTokens[1];
                    gender = key.toString();
                    maxScore = score;
                }
            }
            context.write(new Text(name), new Text("age：" + age + TAB_SEPARATOR + "gender：" + gender + TAB_SEPARATOR + "score：" + maxScore));
        }

    }

    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();

        // 新建一个任务
        Job job =  Job.getInstance(conf, this.getClass().getSimpleName());
        // 输入路径
        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);
        // 主类
        job.setJarByClass(GenderMR.class);
        // Mapper
        job.setMapperClass(GenderMapper.class);
        // map 输出key类型
        job.setMapOutputKeyClass(Text.class);
        // map 输出value类型
        job.setMapOutputValueClass(Text.class);

        // Reducer
        job.setReducerClass(GenderReducer.class);
        // reduce 输出key类型
        job.setOutputKeyClass(Text.class);
        // reduce 输出value类型
        job.setOutputValueClass(Text.class);
        // 设置Combiner类
        job.setCombinerClass(GenderCombiner.class);
        // 设置Partitioner类
        job.setPartitionerClass(GenderPartitioner.class);
        //reduce个数设置为3
        job.setNumReduceTasks(3);

        Path outPath = new Path(args[1]);
        FileSystem fs = outPath.getFileSystem(conf);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        // 输出路径
        FileOutputFormat.setOutputPath(job, outPath);
        // 提交任务
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         args = new String[]{
                "hdfs://com.apache.bigdata:8020/input/gender.txt",
                "hdfs://com.apache.bigdata:8020/output/"};
        int status = ToolRunner.run(new Configuration(), new GenderMR(), args);
        System.exit(status);
    }
}