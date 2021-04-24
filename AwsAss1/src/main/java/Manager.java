
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.Gson;

import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;

public class Manager{
    public static  Map<Integer, Job> jobs = new ConcurrentHashMap<>();
    public static int numOfCurrWorkers=0;
    public static int terminated = 0;
    public static Integer nextJobID = 0;
    public static int nextReviewIndex = 0;
    public static int n=0;
    public static Gson gson = new Gson();


    public static void main(String[] args) {
        System.out.println("Manager Main");
        //todo start mngr to testsqs
        AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n manager is up");// todo delete

        AwsHelper.OpenSQS("SQSresult");
        AwsHelper.OpenSQS("SQSreview");

        ManagerThread responseThread = new ManagerThread();
        Thread thread = new Thread(responseThread);
        thread.start();


        while (true) {
            List<Message> msgs = AwsHelper.popSQS(AwsHelper.sqsLocalsToManager); // check if SQS Queue has new msgs for me

            for (Message m : msgs) {

                String Body = m.body();
                int start = Body.indexOf('[') + 1;
                int end = Body.indexOf(']');
                String[] arguments = Body.substring(start, end).replaceAll("\"", "").split(","); // body String -> String[]
                //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]   (output SQS = outputSQS#jobOwner)

                String address = arguments[0];
                String jobOwner = arguments[1];
                String outputFileName = arguments[2];
                n = Integer.parseInt(arguments[3]);

                // if terminated dont add new Files, but still finish what he got so far
                Job job = downloadAndParse(address, jobOwner, outputFileName);// jobs contains his JobID
                //this job might already been processed. same message can be retrieved twice from the sqs



                //check if job already exists
                if (checkIfJobExists(job)) continue;

                jobs.put(nextJobID++, job); // adds the Job to the jobs Map
                AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n Manager. after jobs.put:"+ jobs.get(nextJobID-1).title); // todo delete

                terminated = Integer.parseInt(arguments[4]); //if local has multiple jobs he needs to send 1 only in the last job !

                //now another thread can check for finished jobs and results


                // getting the estimated amount of currentReviews----------------
                // String attr = "ApproximateNumberOfMessages";// todo fix
                // Map<String, String> attributes = AwsHelper.sqs.getQueueAttributes(new GetQueueAttributesRequest(AwsHelper.getSQSUrl("SQSreview")).withAttributeNames(attr)).getAttributes();
                // int currentReviews = Integer.parseInt(attributes.get(attr));
                int currentReviews = 0;
                //---------------------------------------------------------------


                numOfCurrWorkers = createNewWorkers(job.reviews.size() + currentReviews); // if needed adds new worker instances, checks with SQS size
                pushJobToSQSreview(job);

                if (terminated != 1) {
                    break;
                }

            }
            //now all the messages are handled and can me deleted
            AwsHelper.deletefromSQS(AwsHelper.sqsLocalsToManager, msgs);
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

        int neededWorkers = (int)Math.ceil((float)numOfReviews/n);

        int newWorkers = 0;
        if (neededWorkers>numOfCurrWorkers) {
            newWorkers=neededWorkers-numOfCurrWorkers;
            for (int w=0;w<newWorkers;w++){

                AwsHelper.startInstance("Worker","Worker.jar");
            }
        }
        return newWorkers+numOfCurrWorkers;
    }

    //check if job already exists
    private static boolean checkIfJobExists(Job job){
        for(Job j : jobs.values())
            if (j.isequal(job))
                return true;

        return false;
    }


    private static Job downloadAndParse(String address,String jobOwner, String outputFileName) {

        //------------------download-------------------------------
        try{
            AwsHelper.downloadFile(address,"./"+address);
        }
        catch (Exception ignored){}
        //---------------------parse--------------------------------
        List<Review> reviewList = new LinkedList<>();
        String jobName = "";

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
        Job j = new Job(jobOwner,  nextJobID++, jobName,  reviewList,  outputFileName);
        return j;
    }
    static class ManagerThread implements Runnable {
        @Override
        public void run() {
            AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n manager's thread is up");// todo delete

            while (true) {
                List<Message> results = AwsHelper.popSQS("SQSresult");

                List<Job> finishedJobs = saveResult(results); // if not duplicated, and if Reviews.length=Results.length returns jobID else return -1

                for (Job j : finishedJobs) {
                    AwsHelper.pushSQS("sqsManagerToLocal-" + j.jobOwner, j.outputFileName);
                    jobs.remove(j.jobID);
                }
                if (jobs.isEmpty() && terminated == 1) {
                    AwsHelper.terminateInstancesByTag("Worker"); //numOfCurrWorkers
                    // createResponseMsg();// not sure what that means
                    AwsHelper.terminateInstancesByTag("Manager");
                    break;
                }

            }
        }


        // check if the result is not duplicated, and if Reviews.length=Results.length returns jobID else return -1
        public static List<Job> saveResult(List<Message> results) {
            List<Job> finishedJobs = new LinkedList<>();
            try{

                for (Message m :results){
                    Result r = AwsHelper.fromMSG(m,Result.class);
                    int Jobid = r.jobID;
                    Job job = (Job) jobs.values().toArray()[Jobid];

                    String jobOutputName = job.getOutputFileName();

//                String jobOutputName=jobs.get(r.jobID).outputFileName;

                    int index = r.Reviewindex;
                    String key = jobOutputName + "-" + index;
                    if (AwsHelper.doesFileExists(key)) continue;


                    //-------------------------------
                    //upload file to S3
                    try {
                        File f = new File(key);
                        if (f.createNewFile()) {
                            //success
                            FileWriter myWriter = new FileWriter(key);
                            myWriter.write(gson.toJson(r));
                            myWriter.close();
                            AwsHelper.uploadToS3(key,key);
                        } else {
                            //File already exists
                            AwsHelper.pushSQS(AwsHelper.sqsTesting,"File already exists.");
                        }
                        f.delete();
                    } catch (IOException e) {
                        AwsHelper.pushSQS(AwsHelper.sqsTesting,"File error: " + e);
                        e.printStackTrace();
                    }
                    //-------------------------------

                    //check for last review in job
                    try {// maybe job already finished
                        Job j = (Job) jobs.values().toArray()[r.jobID];

                        j.remainingResponses--;
                        if (j.remainingResponses <= 0) {// finihed with that job
                            finishedJobs.add(j);
                            AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n@@@@@@@@@@@@\nsaveResult: job finished");// todo delete

                        }
                    }
                    catch(Exception e) {
                        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n manager's thread error1:"+ e);// todo delete

                    }
                }

                AwsHelper.deletefromSQS("SQSresult",results);
            }
            catch(Exception e) {
                AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n manager's thread is dead save result error2:"+ e);// todo delete

            }
            return finishedJobs;
        }
    }

}
