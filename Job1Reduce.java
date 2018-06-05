package it.cnr.isti.pad;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class Job1Reduce extends Reducer<Text, Text, Text, Text>{
	
	private Text list = new Text();
	private static String c;
	private static String pgrank;
	private final static String comma = ",";
	private final static String empty = "$";
	
	@Override
	protected void setup(Reducer.Context context) throws IOException, InterruptedException {
 		// Get the number of pages from configuration
		pgrank = context.getConfiguration().get("init_pagerank");
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean g = true;
		HashSet <String> set = new HashSet <String>();
		StringBuilder links = new StringBuilder();
		links.append(pgrank);
		// For each element in "values" the reducer checks if it's '$' (in that case will be
		// skipped) or if it's contained in the HashSet (avoiding all the duplication)
		// if both of them are not satisfied then the element "val" will be put in the outgoing list of page "key"
		for(Text val: values){
			c = val.toString();
			if(!c.equals(empty) && !(set.contains(c))){
				if(g){
					g=false;
					links.append(" ");
				}
				else{
					links.append(comma);
				}
				links.append(c);
				set.add(c);
			}
		}
		// The output of the reducer has the syntax "key  1/n  <OutgoingList>" 
		list.set(links.toString());
		context.write(key,list);
	}
}
