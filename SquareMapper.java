
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
public class SquareMapper extends MapReduceBase implements
Mapper<LongWritable,

Text,

Text, IntWritable> {
//Map function
public void map(LongWritable key, Text value,OutputCollector<Text,IntWritable> output, Reporter rep) throws
IOException
{

	//Splitting the line on spaces
	String line = value.toString();
	for(String word: line.split(" ")) {
	Integer num = Integer.parseInt(word);
	output.collect(new Text("Sum"), new IntWritable(num*num));
	
} 
}
}