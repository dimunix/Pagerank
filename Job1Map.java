package it.cnr.isti.pad;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Job1Map extends Mapper<Object, Text, Text, Text> {
	
	private static String k1,k2 = new String();
	private Boolean g;
	private Text key1 = new Text();
	private Text key2 = new Text();
	private Text value1 = new Text();
	private Text value2 = new Text();
	
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
		StringTokenizer itr = new StringTokenizer(value.toString());
		g = true;
		// Here the mapper controls if the row has at most 2 strings (the two pages)
		if(itr.countTokens()==2){
			k1 = itr.nextToken();
			k2 = itr.nextToken();
			// Controls if this row is a comment (is used char #) and if there is no cycle
			// that is both the pages have the same ID
			if(k1.indexOf('#')==-1 && k2.indexOf('#')==-1 && !(k1.equals(k2))){
				key1.set(k1);
				key2.set(k2);
				value1.set(k2);
				value2.set("$");
				context.write(key1,value1);
				context.write(key2,value2);
			}
		}
	}
	// If all the conditions are satisfied the Mapper's output will be "<pageFrom>  <pageTo>" "<pageTo>  $" 
	
}
