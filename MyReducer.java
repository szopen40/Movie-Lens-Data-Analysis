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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyReducer extends Reducer<Text,Text,Text,Text> {

public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException {
	
	// variables 
	String title = "";
	Integer rate = 0;
    Integer rate_sum = 0;
    Double avg_rating = 0.0;
   	int counter = 0;
    for (Text val :values)
    {
		// data from both mappers are splitted by ':'
        String splitted_data = val.toString();
        String valueSplitted[] = splitted_data.split(":");
		// if code is 'r' then data presents rating 
        if(valueSplitted[1].equals("t"))
			{		
            	NumberFormat _format = NumberFormat.getInstance(Locale.US);
                Number number = null;
                try {
                    number = _format.parse(splitted_data[0].trim());
                    rating = Integer.parseInt(number.toString());
                } catch (ParseException e) {
				}
                // sum of all rates and count number of rates
                rate_sum=rate_sum+rate;
				counter++;
            }
            else 
            {// if code 't' then we will get title assign this value to title variable
            title=valueSplitted[0];
            }
    }
		// if movie was rated more at least 40 times, average rating will be calculated
        if(counter >=40){
        	// calculate the average rating
        	avg_rating=(double)rate_sum/(double)counter;
        	// out put of the first job will be (title," "avg_rating
		context.write(new Text(title), new Text(""+avg_rating));
		}
	}
}

