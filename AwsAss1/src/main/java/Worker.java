
import java.util.*;
import java.util.List;
import com.google.gson.Gson;

import software.amazon.awssdk.services.sqs.model.Message;

public class Worker {

    public static sentimentAnalysisHandler sentiment;
    public static namedEntityRecognitionHandler entity;


    public static void main(String[] args) {
        sentiment = new sentimentAnalysisHandler();
        entity = new namedEntityRecognitionHandler();
        //Running on t2-XL means we have 4 vCPUs.
        for(int i=0; i<4;i++) {
            WorkerThread resultThread = new WorkerThread();
            Thread thread = new Thread(resultThread);
            thread.start();
        }
    }


    static class WorkerThread implements Runnable {
        @Override
        public void run() {
            while (true) {
                //worker pulls a review from reviews_SQS added by the manager, performs necessary algorithms, and returns the result to the manager via results_SQS
                List<Message> reviews = AwsHelper.popSQS("SQSreview"); //maybe array of reviews to work on
                List<Message> results = ProccesReview(reviews);
                //push the result to results_SQS for the manager to continue process it
                AwsHelper.pushSQS("SQSresult", results);
                // remove the review from the jobs_SQS
                AwsHelper.deletefromSQS("SQSreview", reviews);
            }

        }

        private static List<Message> ProccesReview(List<Message>  reviews) {
            List<Message> results = new LinkedList<>();
            for(Message m:reviews) {

                String Body = m.body();
                int start = Body.indexOf('{');
                int end = Body.lastIndexOf('}')+1;
                Body = Body.substring(start,end);
                Gson gson = new Gson();
                Review review = gson.fromJson(Body, Review.class);


                Result result = new Result(review.getJobID(),review.getIndex(),review.getLink());
                String reviewStr = review.getText();
                result.setSentimentAnalysis(sentiment.findSentiment(reviewStr));
                result.setNamedEntityRecognition(entity.findEntities(reviewStr));

                results.add(AwsHelper.toMSG(result));

            }
            return results;
        }
    }


}
