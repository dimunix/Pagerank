package it.cnr.isti.pad;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.io.OutputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.commons.math3.stat.descriptive.*;

public class Pagerank extends Configured implements Tool{
	
	// We use web-BerkStan.txt as web graph, downloaded from http://snap.stanford.edu/data/web-BerkStan.html
	// Variable damping factor, used for computation of Pagerank
	private static final double damp_factor = 0.85;
	// Variable number of pages (nodes) in the web graph
	private static final double npages = 685230.0;
	// Variable convergence factor, the threshold used to check the termination 
	// of pagerank (initial value is 0.0000001, but user can change)
	private static String conv_factor = "0.0000001";
	// Number of reducers
	private final int nreducer = 4;
	
	// List used to create a map: iteration -> statistics (min,max,standard deviation,mean) on Pageranks
	ArrayList<String> statistics;
	
	private void FirstPhase(Configuration conf, String in, String out) throws Exception{
		// In this phase is defined the preliminary job, that reads the webgraph "in" file and generates three
		// new files (directories). The first one will be used by the iterative job, contains for each line the following syntax:
		// <FromPage> <InitialPageRank> <<OutListPages>>
		// where initial Pagerank is "1/npages" and "OutListPages" is the outgoing list of page "FromPage".
		// The second and third will be used by the degree job, contain for each line the following syntax:
		// <Page> <Degree>
		// where "Degree" refers to either in-degree or out-degree of page "Page".
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
		job.setNumReduceTasks(nreducer);
		
		// Defines additional single text based outputs 'indegree', 'outdegree' (used as input by degree job) 
		// and 'out0' (used as input by iterative job).
		MultipleOutputs.addNamedOutput(job, "indegree", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "outdegree", TextOutputFormat.class, LongWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "out0", TextOutputFormat.class, Text.class, Text.class);
		
		job.waitForCompletion(true);
	}
	
	private void DegreeDistr(Configuration conf, String input, String output) throws Exception{
		// This job, given input file "input", will print as output a file/directory containing for each line the following syntax:
		// <Degree> <NumPages>
		// where "Degree" is a long and refers either in-degree or out-degree and "NumPages" refers to the
		// the total number of pages having that degree.
		Job job = Job.getInstance(conf, "JobInDeg");
                job.setJarByClass(Pagerank.class);
		
                Path inputdeg = new Path(input);
                // The file/directory "output" will contain be used as 
                Path outputdeg = new Path(output);
		
		// mapper
                job.setMapperClass(DegMap.class);
                job.setMapOutputKeyClass(LongWritable.class);
                job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
                FileInputFormat.addInputPath(job, inputdeg);
		
		// reducer
                job.setReducerClass(DegReduce.class);
                job.setOutputKeyClass(LongWritable.class);
                job.setOutputValueClass(LongWritable.class);
                FileOutputFormat.setOutputPath(job, outputdeg);
                job.setNumReduceTasks(nreducer);
		
		job.waitForCompletion(true);
	}
	
	private long SecondPhase(Configuration conf, String input, String output) throws Exception{
		// In this phase the job makes the i-th iteration of the pagerank algorithm
		// using "output-i" as the input file/directory and "output-i+1" as the
		// output file/directory. At the end of the computation, "output-i+1" contains
		// all the updated pageranks of each page and "output-i" will be deleted.
		
		Job job = Job.getInstance(conf, "Job2");
		job.setJarByClass(Pagerank.class);
		
		Path curr = new Path(input);
		Path succ = new Path(output);
		
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
		job.setNumReduceTasks(nreducer);
		
		job.waitForCompletion(true);
		
		// Get the Counter CONDITION_SAT used to count the number of pages satisfying the convergence condition
		// at the end of this iteration.
		Counters counters = job.getCounters();
                Counter c1 = counters.findCounter(COUNTER.CONDITION_SAT);
		long result = c1.getValue();
                c1.setValue(0);
		
		// Delete the output file of the previous iteration because it's useless from now
		DeleteFile(conf,curr);
		return result;
	}
	
	private void DeleteFile(Configuration conf, Path directory) throws IOException{
		// Delete the file "directory" from the HDFS
		FileSystem fs = FileSystem.get(conf);
		fs.delete(directory, true);
		fs.close();
	}
	
	private void UpdateStructureData(Configuration conf, String directory,int iteration) throws IOException{
		// It will read files in directory "directory", the iterative job's output, and take from each line
		// the Pagerank regardng a page and will put this value in DescriptiveStructure. When the file has 
		// been red, the statistics obtained from DescriptiveStatistics are inserted in global ArrayList.
		FileSystem fs = FileSystem.get(conf);
		Path fr;
		String line;
		String[] words;
		BufferedReader br;
		DescriptiveStatistics prks = new DescriptiveStatistics();
		DecimalFormat mat = new DecimalFormat("00000");
		for(int i= 0; i < nreducer; i++){
			fr = new Path(directory + "/part-r-" + mat.format(i));
			br = new BufferedReader(new InputStreamReader(fs.open(fr)));
			line=br.readLine();
        	        while (line != null){
                        	words = line.split("\\s+");
				prks.addValue(Double.parseDouble(words[1]));
                        	line=br.readLine();
                	}
                	br.close();
		}
		statistics.add(prks.getMin() + "," + prks.getMax() + "," + prks.getPopulationVariance() + "," + prks.getMean() + "\n");
		prks.clear();
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
		// where after the comma will be at most 20 digits  
		String pgrk = df.format(init_pagerank);
		pgrk = pgrk.replace(",",".");
		String dpf = df.format(damp_factor);
		dpf = dpf.replace(",",".");
		
		conf.set("init_pagerank",pgrk);
		conf.set("damp_factor",dpf);
		conf.set("npages",df.format(npages));
		conf.set("outdir",args[1]);
		if(args.length == 3){
                        conf.set("conv_factor",args[2]);
                }else{
                        conf.set("conv_factor",conv_factor);
                }
		// This array will contain all the statistics, contained in a String, concerning one iteration
		// index i => statistics[i] containts mean, min, max and standard deviation of all pagerank
		//            taken from iteration i 
		statistics = new ArrayList<String>();
		
		FirstPhase(conf,args[0],args[1]);
		
		conf.set("mapred.textoutputformat.separator", " ");
		DegreeDistr(conf,args[1] + "-0/in", "indeg");
		DegreeDistr(conf,args[1] + "-0/out", "outdeg");
		conf.set("mapred.textoutputformat.separator", "\t");
		
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
			// the file "args[1]-i-1"
			if(i==0){
				result = SecondPhase(conf,args[1] + "-0/outputs",args[1] + "-1");
			} else{
				result = SecondPhase(conf,args[1] + "-" + i,args[1] + "-" + (i+1));
			}
			// result contains the number of pages satisfying the convergence condition
			System.out.println("RES = " + result);
			i++;
			// Now will be extracted, from the files generated in this phase, all the
			// statistics concerning the pageranks generated in this phase. 
			UpdateStructureData(conf,args[1] + "-" + i,i);
		}while(result<npages);
		
		// Rename the output of last iteration to args[1], as user wishes
		Path curr = new Path(args[1] + "-" + String.valueOf(i));
                Path endf = new Path(args[1]);
		FileSystem fs = FileSystem.get(conf);
                fs.rename(curr, endf);
		//fs.close();
		
		// Print all statistics regarding pageranks of each iteration on file 'opp.csv'
		// simply scanning the ArrayList
		int num_iter = statistics.size();
		byte[] data;
		//FileSystem fs = FileSystem.get(conf);
		Path out = new Path("opp.csv");
		OutputStream dos = fs.create(out);
		// Print for each iteration the statistics in a csv format and the output will be
		// written in file 'opp.csv'
		for(int j=0;j<num_iter;j++){
			data=(j + "," + statistics.get(j)).getBytes(StandardCharsets.UTF_8);
			dos.write(data);
		}
		dos.close();
		fs.close();
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int exitCode = ToolRunner.run(new Configuration(), new Pagerank(), args);
		System.exit(exitCode);
	}
}
