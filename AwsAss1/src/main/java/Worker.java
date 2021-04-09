public class Worker {


    public static void main(String[] args) {
    WORK();
    }

    private static void WORK() {
        while(true){
            Job = pullJobFromSQS();
            Review[] Reviews = Parse(Job);
            Result  = ProccesReviews();
            uploadResultToSQS(Result);
            removeJobFromSQS(Job);
        }
    }
}
