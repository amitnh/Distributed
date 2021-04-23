public class Result {

    public int jobID;
    public int Reviewindex;// review index in the job
    public int sentiment;
    public String[] entities; // PERSON, LOCATION, ORGANIZATION

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
