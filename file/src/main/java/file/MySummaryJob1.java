package file;

import java.io.IOException;

//import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
//import org.apache.hadoop.hbase.mapreduce.TableReducer;
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class MySummaryJob1 {
	
	public static class MyMapper extends TableMapper<Text, IntWritable>  {
		  public static final byte[] CF = "cf".getBytes();
		  public static final byte[] ATTR1 = "attr1".getBytes();

		  private final IntWritable ONE = new IntWritable(1);
		  private Text text = new Text();

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
		    String val = new String(value.getValue(CF, ATTR1));
		    text.set(val);     // we can only emit Writables...
		    context.write(text, ONE);
		  }
		}
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

		  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		    int i = 0;
		    for (IntWritable val : values) {
		      i += val.get();
		    }
		    context.write(key, new IntWritable(i));
		  }
		}

}
