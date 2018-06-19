package it.cnr.isti.pad;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Job1Reduce extends Reducer<Text, Text, Text, Text>{
	
	// This is the reducer of preliminary job
	private Text list = new Text();
	private static String c, outDir;
	private static String pgrank;
	private final static String comma = ",";
	private final static String empty = "$";
	private MultipleOutputs mos;
	
	@Override
	protected void setup(Reducer.Context context) throws IOException, InterruptedException {
 		// Get the number of pages from configuration
		pgrank = context.getConfiguration().get("init_pagerank");
		mos = new MultipleOutputs(context);
		outDir = context.getConfiguration().get("outdir");
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		boolean g = true;
		HashSet <String> outset = new HashSet <String>();
		HashSet <String> inset = new HashSet <String>();
		StringBuilder links = new StringBuilder();
		links.append(pgrank);
		// For each element in "values" the reducer checks if first char in element is '$':
		// if it's so, the element will be put in 'inset', otherwise it will be put in 'outset'.
		// Moreover before being inserted, check if the element is already contained in related HashSet:
		// if it's so, this will not be inserted (avoiding duplications).

		// 
		int outdegree = 0;
		int indegree = 0;
		for(Text val: values){
			c = val.toString();
			if(c.charAt(0) == '$'){
				c = c.substring(1);
				if(!(inset.contains(c))){
					indegree = indegree + 1;
					inset.add(c);
				}
			}else{
				if(!(outset.contains(c))){
					if(g){
						g=false;
						links.append(" ");
					}
					else{
						links.append(comma);
					}
					links.append(c);
					outset.add(c);
					outdegree = outdegree + 1;
				}
			}
		}
		
		// The output of the reducer has the following syntaxes:
		// "key  1/N  <OutgoingList>" for directory "outputs"
		// "key  out-degree" for directory "out", in files "outdeg"
		// "key  in-degree"  for directory "in", in files "indeg".
		list.set(links.toString());
		String name1 = "in/indeg";
		String name2 = "out/outdeg";
		String name3 = "outputs/out";
		mos.write("outdegree", key, new IntWritable(outdegree),name2);
		mos.write("indegree", key, new IntWritable(indegree),name1);
		mos.write("out0", key, list,name3);
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		mos.close();
	}
}
