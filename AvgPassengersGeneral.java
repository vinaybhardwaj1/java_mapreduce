import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.*;

public class AvgPassengersGeneral {

  public static class OneMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	private final static IntWritable one = new IntWritable(1);


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

	  
                String data = value.toString();
		String[] field = data.split(",", -1);
		int passengers = 0;
		if (null != field && field.length == 18 && field[3].length() >0) {
		passengers=Integer.parseInt(field[3]);
		context.write(new Text(one), new IntWritable(passengers));
		}
    }
  }

public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

		int sum = 0;
		int count = 0;
		for (IntWritable val : values) {
		sum = sum + val.get();
		count = count + 1;
		}
		result.set(sum / count);
		context.write(new Text(key.toString()), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weekly passengers");
    job.setJarByClass(AvgPassengers.class);
    job.setMapperClass(OneMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
