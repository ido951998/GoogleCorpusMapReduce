package com.example.myapp;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.regions.Region;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import software.amazon.awssdk.services.ec2.model.InstanceType;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class emr {
    public static void main(String[] args) throws IOException {
        AWSCredentialsProvider credentials = new ProfileCredentialsProvider("default");

        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard().withCredentials(credentials).withRegion(Regions.US_EAST_1).build();
        HadoopJarStepConfig triGramToNumberHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/TriGramToNumber.jar")
                .withMainClass("com.example.myapp.TriGramToNumber")
                .withArgs("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data", "s3n://resultproject2dsp2023/output/TriGramToNumber");
        HadoopJarStepConfig wordsToRHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/WordsToR.jar")
                .withMainClass("com.example.myapp.WordsToR")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/WordsToR");
        HadoopJarStepConfig nRCalculator0Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/NrCalculator.jar")
                .withMainClass("com.example.myapp.nRCalculator")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/WordsToR/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/NRs/0", "0");
        HadoopJarStepConfig nRCalculator1Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/NrCalculator.jar")
                .withMainClass("com.example.myapp.nRCalculator")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/WordsToR/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/NRs/1", "1");
        HadoopJarStepConfig tRCalculator0Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/TrCalculator.jar")
                .withMainClass("com.example.myapp.tRCalculator")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/TRs/0", "0");
        HadoopJarStepConfig tRCalculator1Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/TrCalculator.jar")
                .withMainClass("com.example.myapp.tRCalculator")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/TRs/1", "1");
        HadoopJarStepConfig tRCalculator_aux0Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/TrCalculator_aux.jar")
                .withMainClass("com.example.myapp.tRCalculator_aux")
                .withArgs("s3n://resultproject2dsp2023/output/TRs/0/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/TRs/0", "0");
        HadoopJarStepConfig tRCalculator_aux1Hadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/TrCalculator_aux.jar")
                .withMainClass("com.example.myapp.tRCalculator_aux")
                .withArgs("s3n://resultproject2dsp2023/output/TRs/1/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/TRs/1", "1");
        HadoopJarStepConfig pCalculatorHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/PCalculator.jar")
                .withMainClass("com.example.myapp.pCalculator")
                .withArgs("s3n://resultproject2dsp2023/output/calculated/TRs/0/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/TRs/1/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/NRs/0/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/NRs/1/part-r-00000", "s3n://resultproject2dsp2023/output/P");
        HadoopJarStepConfig rToWordsHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/RToWords.jar")
                .withMainClass("com.example.myapp.RToWords")
                .withArgs("s3n://resultproject2dsp2023/output/TriGramToNumber/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/RtoWords");
        HadoopJarStepConfig wordsToPHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/WordsToP.jar")
                .withMainClass("com.example.myapp.WordsToP")
                .withArgs("s3n://resultproject2dsp2023/output/calculated/RtoWords/part-r-00000", "s3n://resultproject2dsp2023/output/P/part-r-00000", "s3n://resultproject2dsp2023/output/calculated/WordsToP");
        HadoopJarStepConfig sortHadoop = new HadoopJarStepConfig()
                .withJar("s3n://resultproject2dsp2023/Sort.jar")
                .withMainClass("com.example.myapp.SortArabic")
                .withArgs("s3n://resultproject2dsp2023/output/calculated/WordsToP", "s3n://resultproject2dsp2023/output/final/");

        StepConfig triGramToNumberStep = new StepConfig()
                .withName("triGramToNumber")
                .withHadoopJarStep(triGramToNumberHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig nRCalculator0Step = new StepConfig()
                .withName("nRCalculator0")
                .withHadoopJarStep(nRCalculator0Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig nRCalculator1Step = new StepConfig()
                .withName("nRCalculator1")
                .withHadoopJarStep(nRCalculator1Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig tRCalculator0Step = new StepConfig()
                .withName("tRCalculator0")
                .withHadoopJarStep(tRCalculator0Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig tRCalculator1Step = new StepConfig()
                .withName("tRCalculator1")
                .withHadoopJarStep(tRCalculator1Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig tRCalculator0_auxStep = new StepConfig()
                .withName("tRCalculator0_aux")
                .withHadoopJarStep(tRCalculator_aux0Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig tRCalculator1_auxStep = new StepConfig()
                .withName("tRCalculator1_aux")
                .withHadoopJarStep(tRCalculator_aux1Hadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig pCalculatorStep = new StepConfig()
                .withName("pCalculator")
                .withHadoopJarStep(pCalculatorHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig rToWordsStep = new StepConfig()
                .withName("rToWords")
                .withHadoopJarStep(rToWordsHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig wordsToRStep = new StepConfig()
                .withName("wordsToR")
                .withHadoopJarStep(wordsToRHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig wordsToPStep = new StepConfig()
                .withName("wordsToP")
                .withHadoopJarStep(wordsToPHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");
        StepConfig sortStep = new StepConfig()
                .withName("sort")
                .withHadoopJarStep(sortHadoop)
                .withActionOnFailure("TERMINATE_JOB_FLOW");


        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("MyClusterCreatedFromJava")
                .withReleaseLabel("emr-6.9.0") // specifies the EMR release version label, we recommend the latest release
                .withSteps(triGramToNumberStep, wordsToRStep, nRCalculator0Step, nRCalculator1Step, tRCalculator0Step, tRCalculator1Step, tRCalculator0_auxStep, tRCalculator1_auxStep, pCalculatorStep, rToWordsStep, wordsToPStep, sortStep)
                .withLogUri("s3://resultproject2dsp2023/logs") // a URI in S3 for log files is required when debugging is enabled
                .withServiceRole("EMR_DefaultRole") // replace the default with a custom IAM service role if one is used
                .withJobFlowRole("EMR_EC2_DefaultRole") // replace the default with a custom EMR role for the EC2 instance profile if one is used
                .withInstances(new JobFlowInstancesConfig()
                        .withInstanceCount(8)
                        .withHadoopVersion("3.3.4")
                        .withKeepJobFlowAliveWhenNoSteps(true)
                        .withMasterInstanceType("m4.large")
                        .withSlaveInstanceType("m4.large"));

        RunJobFlowResult result = mapReduce.runJobFlow(request);
        System.out.println("The cluster ID is " + result.toString());
    }
}
