package com.lealife.er;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ER {
	/**
	 * Mapper
	 * @author life
	 *
	 */
	public static class ERMapper extends
			Mapper<Object, Text, Text, Text> {

		private Text key = new Text();
		private Text valueOut = new Text();
        String[] eArr = null;
        
        String source;
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            InputSplit inputSplit = context.getInputSplit();
            source = ((FileSplit)inputSplit).getPath().getName().toString();
        }
		public void map(Object i, Text value, Context context)
				throws IOException, InterruptedException {
            // id 姓名 性别	年龄 地址
			// 以\t分隔
            eArr = value.toString().split("\t");
            if(eArr == null || eArr.length < 5) {
                return;
            }
            // md5下, 规范, 减少key的输出
            // 也可以直接
            // key.set(eArr[1] + eArr[2] + eArr[3]);
            key.set(MD5Hash.digest(eArr[1] + eArr[2] + eArr[3]).toString());
            valueOut.set(source + "\t" + value.toString());
			context.write(key, valueOut);
		}
	}

    /**
     * Reducer
     * @author life
     *
     */
	public static class ERReducer extends
			Reducer<Text, Text, Text, NullWritable> {
        NullWritable nullWritable = NullWritable.get();
        
        private MultipleOutputs<Text, NullWritable> groupOut;
        
        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            groupOut = new MultipleOutputs<Text, NullWritable>(context);
        }
        
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
            // 这里可以检查values个数, 如果=1, 不输出
            String path = key.toString() + "/";
			for (Text val : values) {
                groupOut.write("entityGroup", val, nullWritable, path);
			}
		}
        
        /**
         * 这里必须要关闭, 不然会出错
         */
		protected void cleanup(Context context)throws IOException, InterruptedException {
			groupOut.close();
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "ER");
		job.setJarByClass(ER.class);
        
		job.setMapperClass(ERMapper.class);
		job.setReducerClass(ERReducer.class);
        
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); 
        
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
        
		FileInputFormat.addInputPath(job, new Path("/entityInput"));
		FileOutputFormat.setOutputPath(job, new Path("/entityOutput"));
        
		job.setNumReduceTasks(3);
		
        // 分组输出
		MultipleOutputs.addNamedOutput(job, "entityGroup", TextOutputFormat.class, Text.class, Text.class);
        
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}