 package com.df.KPI2_a;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortingMapper extends Mapper<Object, Text, DoubleWritable, Text>{
	
private String title = "";
private Double avg_rate=0.0;
	
public void map(Object key, Text value, Context context
             ) throws IOException, InterruptedException {
				
		String line = value.toString();
		//output of the first job (title, avg_rate) split it with tab
		String splitted_data[] = line.split("\t");
		//get the title
        title = splitted_data[0].trim();        		
		// get the average rating
        NumberFormat num_format = NumberFormat.getInstance(Locale.US);
        Number number = null;
        try {
            number = num_format.parse(splitted_data[1].trim());
            avg_rate = Double.parseDouble(number.toString());
        } catch (ParseException e) {}
		
		// multiply the key with -1 to get Descending sorted values
        avg_rate = avg_rate*-1;
		// output of the 2nd mapper will be average rating and movie_name
		context.write(new DoubleWritable(avg_rate), new Text(title));

}
}
