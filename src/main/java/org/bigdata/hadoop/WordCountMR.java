package org.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class WordCountMR extends Configured implements Tool {

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text mapOutputKey = new Text();
		private IntWritable mapOutputValue = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			System.out.println("key of mapInput:" + key);
			System.out.println("value of mapInput:" + value);
			String[] strs = line.split(" ");

			for (String str : strs) {
				mapOutputKey.set(str);
				context.write(mapOutputKey, mapOutputValue);
			}
			System.out.println(mapOutputKey);
			System.out.println("=======分隔符=======");
		}
	}
	
	public static class wcCombiner extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable(0);
		//<key,list<1,1,1,1,1...>
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;

			for (IntWritable value : values) {
				System.out.print(value + ",");
				sum += value.get();
			}
			outputValue = new IntWritable(sum);

			context.write(key, outputValue);
		}
		
	}

	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable outputValue = new IntWritable(0);
		//<a list<3,6,7,10>
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			System.out.print("key:" + key + "\t" + "values: lsit< ");
			int sum = 0;

			for (IntWritable value : values) {
				System.out.print(value + ",");
				sum += value.get();
			}
			System.out.print(" >" + "\n");
			outputValue = new IntWritable(sum);

			context.write(key, outputValue);
		}

	}

	public static class myPartition extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text key, IntWritable value, int numPartitions) {
			
			String str = key.toString();
            if(str.matches("[a-z]*")){
                return 0;
            }else {
                return 1;
            }
//            int mpoutkey = Integer.valueOf(str);
//            if (mpoutkey >= 0 && mpoutkey < 4) {
//                return 0;
//            } else if (mpoutkey >= 4 && mpoutkey <= 7) {
//                return 1;
//            } else {
//                return 2;
//            }

		}
	}

	public int run(String[] args) throws Exception {

		Configuration config = new Configuration();

		Job job = Job.getInstance(config, this.getClass().getSimpleName());

		job.setJarByClass(getClass());

		Path inPath = new Path(args[0]);
		FileInputFormat.setInputPaths(job, inPath);

		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		
		job.setNumReduceTasks(2);
		job.setPartitionerClass(myPartition.class);
		//job.setCombinerClass(wcCombiner.class);
		

		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		Path outPath = new Path(args[1]);
		FileSystem fs = outPath.getFileSystem(config);

		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		FileOutputFormat.setOutputPath(job, outPath);

		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		args = new String[] {
//				"hdfs://apache.bigdata.com:8020/input/wc.txt",
//			 "hdfs://apache.bigdata.com:8020/output",
				"E:\\MapReduce\\input\\sort.txt",
				"E:\\mapReduce\\output"
				};

		int status = ToolRunner.run(conf, new WordCountMR(), args);

		System.exit(status);

	}

}
