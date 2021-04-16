import edu.stanford.nlp.io.EncodingPrintWriter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import java.util.*;
import java.util.Base64;
import java.util.List;import java.util.Properties;
import java.util.concurrent.TimeUnit;

import edu.stanford.nlp.ling.CoreAnnotations;import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;import edu.stanford.nlp.ling.CoreLabel;import edu.stanford.nlp.pipeline.Annotation;import edu.stanford.nlp.pipeline.StanfordCoreNLP;import edu.stanford.nlp.rnn.RNNCoreAnnotations;import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;import edu.stanford.nlp.trees.Tree;import edu.stanford.nlp.util.CoreMap;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {

    static sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
    static namedEntityRecognitionHandler namedEntityRecognitionHandler  = new namedEntityRecognitionHandler();

    public static void main(String[] args) {

        while(true){
            //worker pulls a review from reviews_SQS added by the manager, performs necessary algorithms, and returns the result to the manager via results_SQS
            List<Message> reviews = AwsHelper.popSQS("SQSreview"); //maybe array of reviews to work on

            List<Message> results = ProccesReview(reviews);
            //push the result to results_SQS for the manager to continue process it
            AwsHelper.pushSQS("SQSresult",results);
            // remove the review from the jobs_SQS
            AwsHelper.deletefromSQS("SQSreview",reviews);
        }
        
    }

    private static List<Message> ProccesReview(List<Message>  reviews) {
        List<Message> results = new LinkedList<>();
        for(Message m:reviews) {
            Review review = AwsHelper.fromMSG(m, Review.class);
            Result result = new Result(review.getJobID(),review.getIndex());
            String reviewStr = review.getText();
            result.setSentimentAnalysis(sentimentAnalysisHandler.findSentiment(reviewStr));
            result.setNamedEntityRecognition(namedEntityRecognitionHandler.findEntities(reviewStr)); // TODO make findEntities perfect.
            results.add(AwsHelper.toMSG(result));
        }
        return results;
    }



}
