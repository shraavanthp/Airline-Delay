package project1;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class delay1 {
	
	public static class delay1Map extends Mapper<Object,Text,Text, IntWritable> {
		public void map(Object key, Text value, Context context){
			Text Date = new Text();
			try{
				String[] data = value.toString().split(",");
				//System.out.println(value.toString());
				String time = computeTime(data[7]);
				IntWritable totalDelay = new IntWritable();
				int totDelay = getDelay(data);
				Date.set(time);
				totalDelay.set(totDelay);
				
				context.write(Date, totalDelay);
			} catch(Exception ex){
				ex.printStackTrace();
			}
		}
	}
	
	public static int getDelay(String[] fields){
		int sumDelay = 0;
		sumDelay = Integer.parseInt(fields[14])+Integer.parseInt(fields[15]);//+Integer.parseInt(fields[24])+Integer.parseInt(fields[25])+Integer.parseInt(fields[26])+Integer.parseInt(fields[27])+Integer.parseInt(fields[28]);
		return sumDelay;
	}
	
	public static String computeTime(String time){		//Compute the range under which the given time falls under.
		String padded="0000".substring(time.length()) + time;
		DateFormat df = new SimpleDateFormat("HHmm");	
		Calendar cal = Calendar.getInstance();
		int hour = Integer.parseInt(padded.substring(0, 2));
		int minute = Integer.parseInt(padded.substring(2, 4));
		minute = (minute<30)?0:30;
		cal.set(Calendar.HOUR_OF_DAY, hour);
		cal.set(Calendar.MINUTE, minute);
		return df.format(cal.getTime());
	}
	
	public static class delay1Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text time, Iterable<IntWritable> values, Context context){
			int totalCount = 0;
			try {
				for(IntWritable val:values){
					totalCount+=val.get();
				}
				context.write(time,new IntWritable(totalCount));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = new Configuration();
		Job job = Job.getInstance(config, "Delay analysis");
		job.setJarByClass(delay1.class);
		job.setMapperClass(delay1Map.class);
		job.setReducerClass(delay1Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("Data.csv"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
