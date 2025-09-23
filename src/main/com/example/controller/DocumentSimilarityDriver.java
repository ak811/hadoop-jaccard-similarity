package com.example.controller;

import com.example.DocumentSimilarityMapper.*;
import com.example.DocumentSimilarityReducer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DocumentSimilarityDriver {

    // args:
    // 0: input path (documents, one doc per line)
    // 1: output path for doc sizes
    // 2: output path for pair intersections
    // 3: final output path for jaccard similarities
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: DocumentSimilarityDriver <in> <outDocSizes> <outPairCounts> <outJaccard>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // --- Job 1: doc sizes ---
        Job job1 = Job.getInstance(conf, "DocSizes");
        job1.setJarByClass(DocumentSimilarityDriver.class);

        job1.setMapperClass(DocSizeMapper.class);
        job1.setReducerClass(DocSizeReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // --- Job 2: pair intersections ---
        Job job2 = Job.getInstance(conf, "PairIntersections");
        job2.setJarByClass(DocumentSimilarityDriver.class);

        job2.setMapperClass(WordToDocMapper.class);
        job2.setReducerClass(PairIntersectionReducer.class);
        job2.setCombinerClass(SumIntReducer.class);  // optional optimization
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job2, new Path(args[0]));
        TextOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        if (!job2.waitForCompletion(true)) System.exit(1);

        // --- Job 3: Jaccard join + compute ---
        Job job3 = Job.getInstance(conf, "Jaccard");
        job3.setJarByClass(DocumentSimilarityDriver.class);

        job3.setMapperClass(PairCountPassThroughMapper.class);
        job3.setReducerClass(JaccardReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        // cache the doc-sizes output from Job 1
        // Add the first part-* file path; HDFS globbing is fine across parts
        job3.addCacheFile(new Path(args[1] + "/part-r-00000").toUri());

        TextInputFormat.addInputPath(job3, new Path(args[2]));
        TextOutputFormat.setOutputPath(job3, new Path(args[3]));

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
