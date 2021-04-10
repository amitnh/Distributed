import java.util.List;
import java.util.Properties;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class Worker {



    public static void main(String[] args) {
        sentimentAnalysisHandler sentimentAnalysisHandler = new sentimentAnalysisHandler();
        namedEntityRecognitionHandler namedEntityRecognitionHandler = new namedEntityRecognitionHandler();

        while(true){
            //worker pulls a review from reviews_SQS added by the manager, performs necessary algorithms, and returns the result to the manager via results_SQS
            Review review = pullReviewFromSQS(); //maybe array of reviews to work on
            Result result = ProccesReview(review);
            //push the result to results_SQS for the manager to continue process it
            pushResultToSQS(result);
            // remove the review from the jobs_SQS
            removeJobFromSQS(review);
        }


    }

    private static Result ProccesReview(Review review) {
        Result result = new Result();

        String reviewStr = review.getReview();
        result.setSentimentAnalysis(sentimentAnalysisHandler.findSentiment(reviewStr));
        result.setNamedEntityRecognition(namedEntityRecognitionHandler.printEntities(reviewStr));
        return result;
    }



}
