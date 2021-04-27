public class Result {

    public int jobID;
    public int Reviewindex;// review index in the job
    public int sentiment;
    public String[] entities; // PERSON, LOCATION, ORGANIZATION
    public String link;

    public Result(int jobID, int reviewindex, String link) {
        this.jobID = jobID;
        this.link = link;
        Reviewindex = reviewindex;
        this.entities = new String[3];
    }

    public void setSentimentAnalysis(int sentiment) {
        this.sentiment=sentiment;
    }
    public void setNamedEntityRecognition(String[] entities) {
        this.entities=entities;
    }


}
