public class Result {

    protected int jobID;
    protected int Reviewindex;
    //TODO
    protected int sentiment;
    protected String entities;

    public void setSentimentAnalysis(int sentiment) {
        this.sentiment=sentiment;
    }
    public void setNamedEntityRecognition(String entities) {
        this.entities=entities;
    }


}
