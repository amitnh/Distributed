
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.google.gson.Gson;

import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.services.sqs.model.Message;

import static sun.misc.GThreadHelper.lock;

public class Manager {
    public static Map<Integer, Job> jobs;
    public static AtomicInteger numOfCurrWorkers = new AtomicInteger(0);
    public static int terminated = 0;
    public static AtomicInteger nextJobID = new AtomicInteger(0);
    public static AtomicInteger OngoingJobs = new AtomicInteger(0);
    public static AtomicInteger Action = new AtomicInteger(0);
    public static Gson gson = new Gson();
    public static final ReentrantLock TerminateLock = new ReentrantLock();
    public static final ReentrantLock JobsLock = new ReentrantLock();

    public static ExecutorService pool;

    public static void main(String[] args) {
        jobs = new ConcurrentHashMap<>();
        System.out.println("Manager Main");
        //todo start mngr to testsqs
        AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n manager is up");// todo delete

        AwsHelper.OpenSQS("SQSresult");
        AwsHelper.OpenSQS("SQSreview");


        int cores = Runtime.getRuntime().availableProcessors();
        pool  = Executors.newFixedThreadPool(cores);

        for (int i =0;i<cores;i++) {
            ManagerThread sendReceieveTasks = new ManagerThread();
            pool.execute(sendReceieveTasks);
        }

    }
    // push reviews to SQSreview from a Job
    private static void pushJobToSQSreview(Job job) {
        List<Message> list = new LinkedList<>();

        for (Review r:job.reviews){
            list.add(AwsHelper.toMSG(r));
        }
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n pushing new job: jobID-" +job.jobID+ " jobOwner="+ job.jobOwner+ " outputFileName="+job.outputFileName); // todo delete
        AwsHelper.pushSQS("SQSreview",list);
    }


    private static int createNewWorkers(int numOfReviews,int n) {
        int numofworkers = numOfCurrWorkers.get();
        int neededWorkers = (int)Math.ceil((float)numOfReviews/n);
        int newWorkers = 0;
        if (neededWorkers>numofworkers) {
            newWorkers=neededWorkers-numofworkers;
            for (int w=0;w<newWorkers;w++){

                AwsHelper.startInstance("Worker","Worker.jar");
            }
        }
        return newWorkers+numofworkers;
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
        catch (Exception e){
            AwsHelper.pushSQS(AwsHelper.sqsTesting, "\nError downloadAndParse: " + e); // todo delete
        }

        //---------------------parse--------------------------------
        List<Review> reviewList = new LinkedList<>();
        String jobName = "";
        AwsHelper.pushSQS(AwsHelper.sqsTesting, "\ndownloadAndParse next job id before:  " + nextJobID); // todo delete

        int jobid = nextJobID.incrementAndGet()-1;// for concorency
        AwsHelper.pushSQS(AwsHelper.sqsTesting, "\ndownloadAndParse next job id after:  " + jobid); // todo delete

        try (Reader reader = new FileReader("./"+address)) {

            BufferedReader Buffer = new BufferedReader(reader);
            String Line = Buffer.readLine();
            jobName = (new JSONObject(Line)).get("title").toString();
            int NextReviewIndex =0;

            while( Line  != null ){
                JSONObject jsnobject = new JSONObject(Line);
                JSONArray jsonArray = jsnobject.getJSONArray("reviews");
                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject explrObject = jsonArray.getJSONObject(i);
                    Review review = gson.fromJson(String.valueOf(explrObject), Review.class);
                    review.setIndex(NextReviewIndex++);
                    review.setJobID(jobid);
                    reviewList.add(review);

                }
                Line = Buffer.readLine();
            }
            File f = new File("./"+address);// deletes the file from local Instance
            f.delete();
        } catch (IOException ignored) {
            AwsHelper.pushSQS(AwsHelper.sqsTesting, "\ndownloadAndParse error44:  " + ignored); // todo delete

        }
        AwsHelper.pushSQS(AwsHelper.sqsTesting, "\ndownloadAndParse before return Job:  " + jobOwner + " " + jobid+ " "+ jobName+ " "+  outputFileName); // todo delete

        return new Job(jobOwner, jobid, jobName,  reviewList,  outputFileName);
    }
    static class ManagerThread implements Runnable {
        @Override
        public void run() {
            //---------------------------receive----------------------------------------------
            if(Action.compareAndSet(0,1) && terminated !=1) {
                receiveTask();
            }
            //---------------------------send----------------------------------------------
            else{ // Action == 1 or Action == 2
                if(terminated==1)
                    Action.set(2);// only send, not getting new jobs
                else
                    Action.set(0);
                try {
                    sendTask();
                } catch (InterruptedException ignored) {
                    AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n manager's thread sendTask Error: " + ignored);// todo delete

                }
            }

            ManagerThread sendReceiveTasks = new ManagerThread();
            pool.execute(sendReceiveTasks);
        }

        private void sendTask() throws InterruptedException {

            List<Message> results = AwsHelper.popSQS("SQSresult");
            List<Job> finishedJobs = saveResult(results); // if not duplicated, and if Reviews.length=Results.length returns jobID else return -1

            for (Job j : finishedJobs) {
                AwsHelper.pushSQS("sqsManagerToLocal-" + j.jobOwner, j.outputFileName);
                OngoingJobs.decrementAndGet();
            }
            if (OngoingJobs.get() == 0 && terminated == 1) {
                AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n manager's thread terminating");// todo delete
                AwsHelper.terminateInstancesByTag("Worker"); //numOfCurrWorkers
                // delete sqs's
                AwsHelper.deleteSQS(AwsHelper.sqsLocalsToManager);
                AwsHelper.deleteSQS("SQSresult");
                AwsHelper.deleteSQS("SQSreview");


                pool.shutdown();
                pool.awaitTermination(20, TimeUnit.SECONDS);
                AwsHelper.terminateInstancesByTag("Manager");

            }
        }

        private void receiveTask() {

            List<Message> msgs = AwsHelper.popSQS(AwsHelper.sqsLocalsToManager); // check if SQS Queue has new msgs for me
            AwsHelper.deletefromSQS(AwsHelper.sqsLocalsToManager, msgs);

            for (Message m : msgs) {
                AwsHelper.pushSQS(AwsHelper.sqsTesting, "\nreceiveTask Messages2"); // todo delete

                String Body = m.body();
                int start = Body.indexOf('[') + 1;
                int end = Body.indexOf(']');
                String[] arguments = Body.substring(start, end).replaceAll("\"", "").split(","); // body String -> String[]
                //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]   (output SQS = outputSQS#jobOwner)

                String address = arguments[0];
                String jobOwner = arguments[1];
                String outputFileName = arguments[2];
                int n = Integer.parseInt(arguments[3]);
                AwsHelper.pushSQS(AwsHelper.sqsTesting, "\nreceiveTask Messages3 +" + address +jobOwner +outputFileName); // todo delete
                // if terminated dont add new Files, but still finish what he got so far
                Job job = downloadAndParse(address, jobOwner, outputFileName);// jobs contains his JobID
                //this job might already been processed. same message can be retrieved twice from the sqs
                AwsHelper.pushSQS(AwsHelper.sqsTesting, "\nreceiveTask Messages4"); // todo delete


                //check if job already exists
                if (checkIfJobExists(job)) {
                    AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n ?@!?#!?$RE?AS?SAFSDF: " + job.jobID); // todo delete
                    continue;
                }
                jobs.put(job.getJobID(), job); // adds the Job to the jobs Map
                OngoingJobs.incrementAndGet();
                if (Integer.parseInt(arguments[4]) != 0) {
//                    locking terminate so no other thread will be writing to terminated at the same time.
                    TerminateLock.lock();
                    if (terminated == 0)
                        terminated = Integer.parseInt(arguments[4]);
                    else
                        terminated--;
                    TerminateLock.unlock();

                    AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n Manager. terminated: " + terminated); // todo delete
                }
                //now another thread can check for finished jobs and results


                //---------------------------------------------------------------
                JobsLock.lock();
                int currentReviews = 0;
                for (Job j : jobs.values()) { // calculates number of review tasks left
                    currentReviews += j.remainingResponses.get();
                }
                AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n pushJobToSQSreview-currentReviews:" + currentReviews + " - job review size " + job.reviews.size() + "job name: " + job.title); // todo delete
                numOfCurrWorkers.set(createNewWorkers(currentReviews, n)); // if needed adds new worker instances, checks with SQS size
                JobsLock.unlock();

                pushJobToSQSreview(job);

            }

        }


        // check if the result is not duplicated, and if Reviews.length=Results.length returns jobID else return -1
        public static List<Job> saveResult(List<Message> results) {
            List<Job> finishedJobs = new LinkedList<>();
            try{
                for (Message m :results){
                    Result r = AwsHelper.fromMSG(m,Result.class);
                    int Jobid = r.jobID;
                    Job job = getJob(Jobid);

                    String jobOutputName = job.getOutputFileName();

                    int index = r.Reviewindex;
                    String key = jobOutputName + "-" + index;
                    if (AwsHelper.doesFileExists(key)) {
                        AwsHelper.pushSQS(AwsHelper.sqsTesting,"File already exists. continue: key : " + key);//TODO delete
                        continue;
                    }

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
                            AwsHelper.pushSQS(AwsHelper.sqsTesting,"File already exists.");//TODO delete
                        }
                        f.delete();
                    } catch (IOException e) {
                        AwsHelper.pushSQS(AwsHelper.sqsTesting,"File error: " + e);//TODO delete
                        e.printStackTrace();
                    }
                    //-------------------------------

                    //check for last review in job
                    try {// maybe job already finished
                        if(r.jobID!=job.jobID)
                            AwsHelper.pushSQS(AwsHelper.sqsTesting, "\n@@@@@@@@@@@@\nsaveResult: error!");// todo delete

                        int remainingResp = job.remainingResponses.decrementAndGet();
                        if (remainingResp <= 0) {// finihed with that job
                            finishedJobs.add(job);
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
    public static Job getJob(int jobID){
        for(Job j: jobs.values()){
            if(j.jobID==jobID)
                return j;
        }
        return  null;
    }
}
