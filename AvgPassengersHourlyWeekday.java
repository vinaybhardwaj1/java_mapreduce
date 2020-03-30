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

public class AvgPassengersHourlyWeekday {

  public static class DatePaserMapper
       extends Mapper<Object, Text, Text, IntWritable>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		
		if (value.toString().contains("passenger_count")) // for skipping header
                return;
            else {
                String data = value.toString();
				String[] field = data.split(",", -1);
				int passengers = 0;
				Date pickupdate;
				if (null != field && field.length == 18 && field[3].length() >0) {
				passengers=Integer.parseInt(field[3]); //picking passenger_count field
				String dayofweek = "";
				try{
				pickupdate = new SimpleDateFormat("dd-MM-yyyy hh:mm").parse(field[1]);
				dayofweek = new SimpleDateFormat("EEEE").format(pickupdate).toString(); // parsing date to day of week
				hourofday = new SimpleDateFormat("H").format(pickupdate).toString(); //parsing date to hour of day
				}catch(Exception e){
					System.out.println(e);
				}
				if(dayofweek == "Monday" || dayofweek == "Tuesday" || dayofweek == "Wednesday" || dayofweek == "Thursday" || dayofweek == "Friday"){
				context.write(new Text(hourofday), new IntWritable(passengers));
				}
            }
		
	  
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
    job.setJarByClass(AvgPassengersHourlyWeekday.class);
    job.setMapperClass(DatePaserMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
