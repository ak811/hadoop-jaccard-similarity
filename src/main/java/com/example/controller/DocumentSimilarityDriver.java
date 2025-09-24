package com.example.controller;

import com.example.DocumentSimilarityMapper.*;
import com.example.DocumentSimilarityReducer.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

        // --- Job 1: compute doc sizes (unique word count per doc) ---
        Job job1 = Job.getInstance(conf, "DocSizes");
        job1.setJarByClass(DocumentSimilarityDriver.class);

        job1.setMapperClass(DocSizeMapper.class);
        job1.setReducerClass(DocSizeReducer.class);

        // mapper outputs (docId, 1)
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        // reducer outputs (docId, size)
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        if (!job1.waitForCompletion(true)) System.exit(1);

        // --- Job 2: build pairs via inverted index, emit (pair -> 1) per shared word ---
        Job job2 = Job.getInstance(conf, "PairIntersections");
        job2.setJarByClass(DocumentSimilarityDriver.class);

        job2.setMapperClass(WordToDocMapper.class);
        job2.setReducerClass(PairIntersectionReducer.class);
        // IMPORTANT: DO NOT set a combiner here; mapper emits Text values.
        // job2.setCombinerClass(SumIntReducer.class); // ❌ WRONG for this job (mapper value is Text)

        // mapper outputs (word, docId) -> (Text, Text)
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        // reducer outputs (docA,docB, 1) -> (Text, IntWritable)
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        TextInputFormat.addInputPath(job2, new Path(args[0]));
        TextOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        if (!job2.waitForCompletion(true)) System.exit(1);

        // --- Job 3: sum intersections per pair and compute Jaccard using cached doc sizes ---
        Job job3 = Job.getInstance(conf, "Jaccard");
        job3.setJarByClass(DocumentSimilarityDriver.class);

        job3.setMapperClass(PairCountPassThroughMapper.class);
        job3.setReducerClass(JaccardReducer.class);

        // mapper outputs (pair, count)
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        // reducer outputs ("A, B", "Similarity: 0.56")
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        // Cache the single reducer output from Job 1 (part-r-00000) with (docId \t size)
        job3.addCacheFile(new Path(args[1] + "/part-r-00000").toUri());

        TextInputFormat.addInputPath(job3, new Path(args[2]));
        TextOutputFormat.setOutputPath(job3, new Path(args[3]));
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
