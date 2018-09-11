package project1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class delay2 {

	public static class delay2Map extends Mapper<Object,Text,Text, IntWritable> {
		public void map(Object key, Text value, Context context){
			Text Carrier = new Text();
			try{
				String[] data = value.toString().split(",");
				Carrier.set(data[8]);
				IntWritable totalDelay = new IntWritable();
				//System.out.println(value.toString());
				int totDelay = delay1.getDelay(data);
				totalDelay.set(totDelay);
				context.write(Carrier, totalDelay);
				
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public static class delay2Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text time, Iterable<IntWritable> values, Context context){
			int totalCount = 0;
			try {
				for(IntWritable val:values){
					totalCount++;
				}
				context.write(time,new IntWritable(totalCount));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Delay analysis 2");
		job.setJarByClass(delay2.class);
		job.setMapperClass(delay2Map.class);
		job.setReducerClass(delay2Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("Dataset.csv"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
