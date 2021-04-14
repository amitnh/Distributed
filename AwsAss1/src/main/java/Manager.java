import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;

import java.io.*;
import java.util.*;
import java.util.Base64;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.stream.JsonReader;
import org.json.JSONArray;
import org.json.JSONObject;
import sun.awt.image.ImageWatched;

public class Manager implements Runnable{
    static  Map<Integer, Job> jobs = new HashMap<>();
    static int numOfCurrWorkers=0;
    static int terminated = 0;
    static int nextJobID = 0;


    public static void main(String[] args) {

        final int n = Integer.parseInt(args[0]); // getting it throgh the localApplication args

        Manager responseThread = new Manager();
        Thread thread = new Thread(responseThread);
        thread.start();

        while(true) { // maybe sleep?
            String[] arguments = checkLocalAppSqs(); // check if SQS Queue has new msgs for me
            //arguments  -> [address, jobOwner, outputFileName]   (output SQS = outputSQS#jobOwner)
            String address = arguments[0];
            String jobOwner = arguments[1];
            String outputFileName = arguments[2];

            if (address != null && terminated !=1) { // if terminated dont add new Files, but still finish what he got so far
                Job job = downloadAndPharse(address,nextJobID,jobOwner, outputFileName);// jobs contains his JobID
                jobs.put(nextJobID++,job); // adds the Job to the jobs Map

                numOfCurrWorkers = createNewWorkers(job.reviews.size(),numOfCurrWorkers); // if needed adds new worker instances, checks with SQS size
                sendReviewsToWorkersSqs(job);
            }
        }

    }
    private static Job downloadAndPharse(String address, int nextJobID,String jobOwner, String outputFileName) {
        //TODO download adress from S3
        List<Review> reviewList = new LinkedList<>();
        String jobName = "";
        Gson gson = new Gson();
        try (Reader reader = new FileReader(address)) {
            BufferedReader Buffer = new BufferedReader(reader);
            String Line = Buffer.readLine();
            jobName = (new JSONObject(Line)).get("title").toString();

            while( Line  != null){
                JSONObject jsnobject = new JSONObject(Line);
                JSONArray jsonArray = jsnobject.getJSONArray("reviews");
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject explrObject = jsonArray.getJSONObject(i);
                    Review review = gson.fromJson(String.valueOf(explrObject), Review.class);
                    reviewList.add(review);
                }
                Line = Buffer.readLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
//    public Job(String jobOwner, int jobID, String title, Review[] reviews,  String outputFileName) {

        Job job = new Job(jobOwner,  nextJobID, jobName,  reviewList,  outputFileName);
        return job;
    }

    @Override
    public void run() {
        while (true) {
            /// maybe another Thread
            Result result = checkResultsSqs();
            if (result != null) {
                int finishedJobID = saveResult(result); // if not duplicated, and if Reviews.length=Results.length returns jobID else return -1
            }

            if (finishedJobID != -1) {  // finished
                string address = uploadResultsToS3(finishedJobID);
                sendToLocalAppSQS(address); // updating the localapp sqs for the new result
                if (terminated == 1) {
                    terminateAllWorkers(); //numOfCurrWorkers
                    createResponseMsg();// not sure what that means
                    terminate();
                }
            }
        }
    }
}
