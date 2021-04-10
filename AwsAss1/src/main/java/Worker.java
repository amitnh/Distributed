public class Worker {


    public static void main(String[] args) {
    WORK();
    }

    private static void WORK() {
        while(true){
            //worker pulls a review from reviews_SQS added by the manager, performs necessary algorithms, and returns the result to the manager via results_SQS
            Review review = pullReviewFromSQS(); //maybe array of reviews to work on
            Result result = ProccesReview();
            //push the result to results_SQS for the manager to continue process it
            pushResultToSQS(result);
            // remove the review from the jobs_SQS
            removeJobFromSQS(review);
        }
    }
}
