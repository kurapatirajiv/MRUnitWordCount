package com.cloudwick.tests;


import com.cloudwick.source.WordCountMapper;
import com.cloudwick.source.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rajiv on 3/27/17.
 */
public class MRUnitTestsWC {

    MapDriver<Object, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {

        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();

        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("abc,def"));
        mapDriver.withOutput(new Text("abc"), new IntWritable(1));
        mapDriver.withOutput(new Text("def"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void testReducer() throws IOException {

        List<IntWritable> myValues = new ArrayList<IntWritable>();

        myValues.add(new IntWritable(1));
        myValues.add(new IntWritable(1));
        reduceDriver.withInput(new Text("abc"), myValues);
        reduceDriver.withInput(new Text("def"), myValues);
        reduceDriver.withOutput(new Text("abc"), new IntWritable(2));
        reduceDriver.withOutput(new Text("def"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {

        mapReduceDriver.withInput(new LongWritable(), new Text("abc,def,abc"));
        mapReduceDriver.withOutput(new Text("abc"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("def"), new IntWritable(1));

        // Fail Test case
        //mapReduceDriver.withOutput(new Text("fail Case"), new IntWritable(3));

        mapReduceDriver.runTest();

    }

}
