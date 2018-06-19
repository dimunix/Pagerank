package it.cnr.isti.pad;

import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Map extends Mapper<Text, Text, Text, Text> {
	// This is the mapper of Iterative job
		
        private static String k, temp, list = new String();
        private Text key1 = new Text();
        private Text value1 = new Text();
	private static String[] parts;
	
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException{
		// Because of KeyValueTextInputFormat as InputFormat, the mapper has "key" the page in first column
		// the row, the "value" is the Pagerank and, maybe, its outgoing list of pages 
		k = value.toString();
		DecimalFormat df = new DecimalFormat("#.#");
                df.setMaximumFractionDigits(20);
		// The Mapper checks if the "value" has at least one element, the "value" has the form
		// "PagerankKey <OutgoingListKey>" and <OutgoingListKey> could be empty
		if(k.indexOf(" ")>=0){
			double pgrk = 0.0;
			parts = k.split(" ");
			pgrk = Double.parseDouble(parts[0]);
			list = parts[1];
			parts = list.split(",");
			pgrk = pgrk/parts.length;
			temp = df.format(pgrk);
			temp = temp.replace(",",".");
			// If the <OutgoingListKey> has at least one element then the Mapper generates one row
			// for each element in the list with the syntax "OutPage PagerankKey/<OutgoingListKey>.length"
			for(int i=0; i<parts.length; i++){
				key1.set(parts[i]);
				value1.set(temp);
				context.write(key1,value1);
			}
			// At the bottom line the Mapper produces as output the row "key PagerankKey#<OutgoinListKey>"
			k = k.replace(" ","#");	
		} else{
			// If there isn't any element in <OutgoingListKey> then the Mapper produces as output the row "key PagerankKey#"
			k = k + "#";
		}
		value1.set(k);
		context.write(key,value1);
	}

}
