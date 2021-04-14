public class Result {

    protected int jobID;
    protected int Reviewindex;// review index in the job
    protected int sentiment;
    protected String[] entities; // PERSON, LOCATION, ORGANIZATION

    public Result(int jobID, int reviewindex) {
        this.jobID = jobID;
        Reviewindex = reviewindex;
    }

    public void setSentimentAnalysis(int sentiment) {
        this.sentiment=sentiment;
    }
    public void setNamedEntityRecognition(String[] entities) {
        this.entities=entities;
    }


}
