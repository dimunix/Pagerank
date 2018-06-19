package it.cnr.isti.pad;

import java.io.IOException;
import java.lang.Long;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DegMap extends Mapper<Object, Text, LongWritable, LongWritable> {
	
	// This is the mapper of Degree job	
        private final static LongWritable one = new LongWritable(1);
	//private final static Text one = new Text("1");
	private static LongWritable prova = new LongWritable();
	private static String val;
	private static String[] arr;
	
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		// The mapper extracts from the 'value' the page and its (in/out-)degree and then
		// it prints as 'key' the degree and as 'value' the 1 as LongWritable
                val = value.toString();
		arr = val.split("\\s+");
		prova.set(Long.parseLong(arr[1]));
		context.write(prova,one);
		
        }
}
