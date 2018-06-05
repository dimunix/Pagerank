package it.cnr.isti.pad;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Pagerank extends Configured implements Tool{
	
	// We use web-BerkStan.txt as web graph, downloaded from http://snap.stanford.edu/data/web-BerkStan.html
	// Variable damping factor, used for computation of pagerank
	private static final double damp_factor = 0.85;
	// Variable number of pages (nodes) in the web graph
	private static final double npages = 685230.0;
	// Variable convergence factor, the threshold used to check the termination 
	// of pagerank (initial value is 0.00000001, but user can change)
	private static String conv_factor = "0.00000001";
	
	
	private void FirstPhase(Configuration conf, String in, String out) throws Exception{
		// In this phase the job reads the webgraph "in" file and generates a
		// new file (directory) "out-0" containing for each line the following syntax:
		// <FromPage> <InitialPageRank> <<OutListPages>>
		// where initial Pagerank is "1/npages" and "OutListPages" is the list of
		// outgoing page of page "FromPage".
		
		Job job = Job.getInstance(conf, "Job1");
		job.setJarByClass(Pagerank.class);
		
		Path webgraph = new Path(in);
		// The file "out-0" will be used as input in the first iteration of Pagerank
		Path output = new Path(out + "-0");
		
		// mapper
		job.setMapperClass(Job1Map.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, webgraph);
		
		// reducer
		job.setReducerClass(Job1Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, output);
		
		job.waitForCompletion(true);
	}
	
	private long SecondPhase(Configuration conf, String output, int i) throws Exception{
		// In this phase the job makes the i-th iteration of the pagerank algorithm
		// using "output-i" as the input file/directory and "output-i+1" as the
		// output file/directory. At the end of the computation, "output-i+1" contains
		// all the updated pageranks of each page and "output-i" will be deleted.
		
		Job job = Job.getInstance(conf, "Job2");
		job.setJarByClass(Pagerank.class);
		
		Path curr = new Path(output + "-" + String.valueOf(i));
		Path succ = new Path(output + "-" + String.valueOf(i+1));
		
		// mapper
                job.setMapperClass(Job2Map.class);
		job.setInputFormatClass(KeyValueTextInputFormat.class);
                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, curr);
		
		// reducer
		job.setReducerClass(Job2Reduce.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileOutputFormat.setOutputPath(job, succ);
		
		job.waitForCompletion(true);
		
		// Counter used to count the number of pages satisfying the convergence condition
		Counters counters = job.getCounters();
                Counter c1 = counters.findCounter(COUNTER.CONDITION_SAT);
		long result = c1.getValue();
                c1.setValue(0);
		
		// Delete the output file of the previous iteration
		DeleteFile(conf,curr);
		
		return result;
	}
	
	private void DeleteFile(Configuration conf, Path directory) throws IOException{
		// Delete the file "directory" from the HDFS
		FileSystem fs = FileSystem.get(conf);
		fs.delete(directory, true);
		fs.close();
	}
	
	@Override
	public int run(String[] args) throws Exception{
		if(args.length != 2 && args.length != 3){
			System.out.println("Usage: Pagerank <webgraph> <output> <convergence-factor>");
			System.out.println("<convergence-factor> is optional");
			return 2;
		}
		
		Configuration conf = this.getConf();
		// Set initial pagerank as global variable for every mapper and reducer of each phase
		// but it's used only for the first phase
		double init_pagerank = (double) 1/npages;
		DecimalFormat df = new DecimalFormat("#.#");
		df.setMaximumFractionDigits(20);
		
		// The Pagerank will be represented as a string not in notation floating point
		// where after the comma will be at most 20 numbers  
		String pgrk = df.format(init_pagerank);
		pgrk = pgrk.replace(",",".");
		String dpf = df.format(damp_factor);
		dpf = dpf.replace(",",".");
		
		conf.set("init_pagerank",pgrk);
		conf.set("damp_factor",dpf);
		conf.set("npages",df.format(npages));
		if(args.length == 3){
                        conf.set("conv_factor",args[2]);
                }else{
                        conf.set("conv_factor",conv_factor);
                }
		
		FirstPhase(conf,args[0],args[1]);
		// It's the pagerank's computation iterated until all the pages
		// satisfies the convergence condition, that is the following:
		//       | PRi(A) - PRi+1(A) | < e
		// where A is a generic page, PRi(A) is the pagerank of A at the
		// i-th iteration, and e is the error's threshold. This threshold
		// is computed using the counter CONDITION_SAT, that counts all the
		// pages satisfiyng the condition described.
		long result = 0;
		int i = 0;
		do{
			// Generate an output file with the format args[1]"-i", deleting
			// the file args[1]"-i-1"
			result = SecondPhase(conf,args[1],i);
			//System.out.println("RES = " + result);
			i++;
		}while(result<npages);
		
		// Rename the output of last iteration to args[1], as wished
		Path curr = new Path(args[1] + "-" + String.valueOf(i));
                Path endf = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
                fs.rename(curr, endf);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new Pagerank(), args);
		System.exit(exitCode);
	}

}
