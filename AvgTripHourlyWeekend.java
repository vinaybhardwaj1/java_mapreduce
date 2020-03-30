import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.text.SimpleDateFormat;
import java.util.*;

public class AvgTripHourlyWeekend {

  public static class DatePaserMapper
       extends Mapper<Object, Text, Text, FloatWritable>{


    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
		
		if (value.toString().contains("passenger_count")) // for skipping header
                return;
            else {
                String data = value.toString();
				String[] field = data.split(",", -1);
				float trip = 0;
				Date pickupdate;
				if (null != field && field.length == 18 && field[4].length() >0) {
				trip=Float.parseFloat(field[4]); // picking up trip_distance field
				String dayofweek = "";
				try{
				pickupdate = new SimpleDateFormat("dd-MM-yyyy hh:mm").parse(field[1]);
				dayofweek = new SimpleDateFormat("EEEE").format(pickupdate).toString(); // parsing date to day of week
				hourofday = new SimpleDateFormat("H").format(pickupdate).toString(); //parsing date to hour of day
				}catch(Exception e){
					System.out.println(e);
				}
				if(dayofweek == "Saturday" || dayofweek == "Sunday" ){
				context.write(new Text(hourofday), new FloatWritable(trip));
				}
            }
		
	  
      }
    }
  }

public static class IntSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

		float sum = 0;
		int count = 0;
		for (float val : values) {
		sum = sum + val.get();
		count = count + 1;
		}
		result.set(sum / count);
		context.write(new Text(key.toString()), result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "weekly trip distance");
    job.setJarByClass(AvgTripHourlyWeekend.class);
    job.setMapperClass(DatePaserMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
