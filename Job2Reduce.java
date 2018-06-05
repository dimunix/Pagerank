package it.cnr.isti.pad;

import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DecimalFormat;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Reducer;

public class Job2Reduce extends Reducer<Text, Text, Text, Text>{

        private Text list = new Text();
        private static String c;
        private static double conv_factor, damp_factor, npages;
	
        @Override
        protected void setup(Reducer.Context context) throws IOException, InterruptedException {
                // Get the damping factor, the number of pages and the convergence threshold from the context
                damp_factor = Double.parseDouble(context.getConfiguration().get("damp_factor"));
		npages = Double.parseDouble(context.getConfiguration().get("npages"));
		conv_factor = Double.parseDouble(context.getConfiguration().get("conv_factor"));
        }
	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double newpgrank, oldpgrank = 0.0;
		String[] outlist = null;
		boolean b = false;
		DecimalFormat df = new DecimalFormat("#.#");
                df.setMaximumFractionDigits(20);
		double diff, sum = 0.0;
		String tmp;
		int i = 0;
		for(Text val: values){
			c = val.toString();
			if(c.indexOf("#")==-1){
				// This is the case where we have only the pagerank (divided
				// by the outgoing list length) of a page pointed by "key"
				double v = Double.parseDouble(c);
				sum = sum + v;
			} else{
				// This is the case where there's the old pagerank
				// and (maybe) the list of all outgoing pages from "key"
				outlist = c.split("#");
				// Here the reducer checks if it has the outgoing links list and, if it's
				// the case, it will be in outlist[1] (and this is denoted by boolean b) 
				if(outlist.length >1){
					b = true;
				}
				oldpgrank = Double.parseDouble(outlist[0]);
			}
			i++;
		}
		if(i>1){
			// If there's at least one page pointing to "key" the new Pagerank will be computed
			// Pagerank iterative formula
			newpgrank = (damp_factor*(sum)) + ((1-damp_factor)/npages);
			tmp = df.format(newpgrank);
			// Write the result in fixed notation
			tmp = tmp.replace(",",".");
			if(b){
				// The page has at least one element in outgoing list
				list.set(tmp + " " + outlist[1]);
			} else{
				list.set(tmp);
			}
			// Difference used for the convergence condition
			diff = Math.abs(newpgrank - oldpgrank);
		} else{
			// If there isn't any page pointing to "key" than the Pagerank doesn't change
			tmp = df.format(oldpgrank);
                        tmp = tmp.replace(",",".");
			if(b){
				// The page has at least one element in outgoing list
				list.set(tmp + " " + outlist[1]);
			} else{
				list.set(tmp);
			}
			// In this case the Pagerank isn't changed
			diff = 0.0;
		}
		// Convergence condition
		if(diff < conv_factor){
			context.getCounter(COUNTER.CONDITION_SAT).increment(1);
		}
		// The reducer writes as output a row with syntax "key NewPagerankKey <OutgoingListKey>"
		context.write(key,list);
	}
	
}
