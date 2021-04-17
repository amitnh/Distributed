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
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import sun.awt.image.ImageWatched;

public class Manager implements Runnable{
    public static  Map<Integer, Job> jobs = new HashMap<>();
    public static int numOfCurrWorkers=0;
    public static int terminated = 0;
    public static int nextJobID = 0;
    public static int nextReviewIndex = 0;
    public static int n=0;
    public static String s3name = "bucket-amitandtal";

    public static void main(String[] args) {
        //todo start mngr to testsqs
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n manager is up");// todo delete
        Manager responseThread = new Manager();
        Thread thread = new Thread(responseThread);
        thread.start();

        while(true) { // maybe sleep?
            List<Message> msgs = AwsHelper.popSQS(AwsHelper.sqsLocalsToManager); // check if SQS Queue has new msgs for me

            for (Message m : msgs) {
                String[] arguments = AwsHelper.fromMSG(m,String[].class);
                //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]   (output SQS = outputSQS#jobOwner)
                String address = arguments[0];
                String jobOwner = arguments[1];
                String outputFileName = arguments[2];
                n = Integer.parseInt(arguments[3]);
                terminated = Integer.parseInt(arguments[4]); //if local has multiple jobs he needs to send 1 only in the last job !

                 // if terminated dont add new Files, but still finish what he got so far
                    Job job = downloadAndParse(address, jobOwner, outputFileName);// jobs contains his JobID
                    jobs.put(nextJobID++, job); // adds the Job to the jobs Map

                // getting the estimated amount of currentReviews----------------
                // String attr = "ApproximateNumberOfMessages";// todo fix
               // Map<String, String> attributes = AwsHelper.sqs.getQueueAttributes(new GetQueueAttributesRequest(AwsHelper.getSQSUrl("SQSreview")).withAttributeNames(attr)).getAttributes();
               // int currentReviews = Integer.parseInt(attributes.get(attr));
                int currentReviews = 0;
                //---------------------------------------------------------------
                    numOfCurrWorkers = createNewWorkers(job.reviews.size()+currentReviews); // if needed adds new worker instances, checks with SQS size
                pushJobToSQSreview(job);
                if (terminated != 1) {
                    break;
                }
            }
        }
    }
    // push reviews to SQSreview from a Job
    private static void pushJobToSQSreview(Job job) {
        List<Message> list = new LinkedList<>();
        for (Review r:job.reviews){
            list.add(AwsHelper.toMSG(r));
        }
        AwsHelper.pushSQS("SQSreview",list);
    }

    private static int createNewWorkers(int numOfReviews) {
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n createNewWorkers"); // todo delete

        int neededWorkers = (int)Math.ceil(numOfReviews/n);
        int newWorkers = 0;
        if (neededWorkers>numOfCurrWorkers) {
            newWorkers=numOfCurrWorkers-neededWorkers;
            for (int w=0;w<newWorkers;w++){
                AwsHelper.startInstance("Worker","Worker.jar");
            }
        }
        return newWorkers;
    }

    private static Job downloadAndParse(String address,String jobOwner, String outputFileName) {
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n downloadAndParse: "+ address); // todo delete

        //------------------download-------------------------------
        try{
            AwsHelper.downloadFile(address,"./"+address);
        }
        catch (Exception e){}
        //---------------------parse--------------------------------
        List<Review> reviewList = new LinkedList<>();
        String jobName = "";
        Gson gson = new Gson();

        try (Reader reader = new FileReader("./"+address)) {
            BufferedReader Buffer = new BufferedReader(reader);
            String Line = Buffer.readLine();
            jobName = (new JSONObject(Line)).get("title").toString();

            while( Line  != null){
                JSONObject jsnobject = new JSONObject(Line);
                JSONArray jsonArray = jsnobject.getJSONArray("reviews");
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject explrObject = jsonArray.getJSONObject(i);
                    Review review = gson.fromJson(String.valueOf(explrObject), Review.class);
                    review.setIndex(nextReviewIndex++);
                    reviewList.add(review);

                }
                Line = Buffer.readLine();
            }
            File f = new File("./"+address);// deletes the file from local Instance
            f.delete();
        } catch (IOException e) {
            e.printStackTrace();
        }
            return new Job(jobOwner,  nextJobID++, jobName,  reviewList,  outputFileName);
    }

    @Override
    public void run() {
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n manager's thread is up");// todo delete

        while (true) {
            List<Message> results = AwsHelper.popSQS("SQSresult");
            List<Job> finishedJobs = saveResult(results); // if not duplicated, and if Reviews.length=Results.length returns jobID else return -1

            for (Job j:finishedJobs){
                AwsHelper.pushSQS(AwsHelper.sqsLocalsToManager + j.jobOwner,j.outputFileName);
                jobs.remove(j.jobID);
            }
            if (jobs.isEmpty() && terminated == 1) {
                terminateAllWorkers(); //numOfCurrWorkers
                createResponseMsg();// not sure what that means
                terminate();
            }

        }
    }

    // check if the result is not duplicated, and if Reviews.length=Results.length returns jobID else return -1
    private List<Job> saveResult(List<Message> results) {
        List<Job> finishedJobs = new LinkedList<>();
        for (Message m :results){
            Result r = AwsHelper.fromMSG(m,Result.class);

            //check fo duplication
            String jobOutputName=jobs.get(r.jobID).outputFileName;
            int index = r.Reviewindex;
            String key = jobOutputName + "/" + index;
            if (AwsHelper.doesFileExists(key)) continue;

            //upload file to S3
            File f = new File("/"+key);
            AwsHelper.uploadToS3("/"+key,key);
            f.delete();

            //check for last review in job
            try {// maybe job already finished
                Job j = jobs.get(r.jobID);
                j.remainingResponses--;
                if (j.remainingResponses <= 0)// finihed with that job
                    finishedJobs.add(j);
            }
            catch(Exception e) {}
        }

        AwsHelper.deletefromSQS("SQSreview",results);

        return finishedJobs;
    }
}
