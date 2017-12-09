package file;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.FileOutputFormat;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import file.MySummaryJob1.MyMapper;
import file.MySummaryJob1.MyReducer;
//import file.MySummaryJob1.MyTableReducer;

public class Driver1 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		@SuppressWarnings("deprecation")
		//Job job =Job.getInstance(config,"ExampleSummary");
		Job job = new Job(config,"ExampleSummaryToFile");
		job.setJarByClass(MySummaryJob1.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
		  "test",        // input table
		  scan,               // Scan instance to control CF and attribute selection
		  MyMapper.class,     // mapper class
		  Text.class,         // mapper output key
		  IntWritable.class,  // mapper output value
		  job);
		/*TableMapReduceUtil.initTableReducerJob(
		  "test1",        // output table
		  MyReducer.class,    // reducer class
		  job);
		job.setNumReduceTasks(1);   // at least one, adjust as required
*/
		job.setReducerClass(MyReducer.class);    // reducer class
		job.setNumReduceTasks(1);    // at least one, adjust as required
		FileOutputFormat.setOutputPath(job,new Path("/user/root/mySummaryFile"));
		boolean b = job.waitForCompletion(true);
		if (!b) {
		  throw new IOException("error with job!");
		}
	}
}
