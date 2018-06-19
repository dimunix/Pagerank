package it.cnr.isti.pad;

import java.io.IOException;
import java.lang.StringBuilder;
import java.lang.Long;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class DegReduce extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{
	
	// This is the reducer of Degree Job
        private LongWritable result = new LongWritable();
	private Text l = new Text();
	
        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		// The reducer collects all the 1s, contained in 'values',  associated to 'key' and sums them
                long sum = 0;
		for(LongWritable val: values){
			sum = sum + val.get();
		}
		result.set(sum);
		// Eventually the reducer will print as output the (in/out-)degree, as 'key', and the number of 
		// pages with this (in/out-)degree, as 'value'.
		context.write(key,result);
        }
}
