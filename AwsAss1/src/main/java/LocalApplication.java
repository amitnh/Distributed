import java.awt.*;
import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;


public class LocalApplication {


    private static long myID;
    private static int jobsCounter=-1;
    private static int terminated=0;
    public static String access_key_id = "";
    public static String secret_access_key_id = "";
    //private static AwsBasicCredentials awsCreds = AwsBasicCredentials.create(access_key_id, secret_access_key_id);
    private static String sqsManagerToLocal;
    private static String sqsLocalsToManager = "sqsLocalsToManager";
    private static String sqsTesting = "sqsTesting";
    //args[] = [inputfilename1, ..., inputfilenameN, outputfilename1,..., outputfilenameN, n, terminate]
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("Local Main");
        AwsHelper.OpenSQS(sqsTesting);      //, this SQS is for ALL locals to upload jobs for the manager

        int numOfFiles = (args.length-2)/2;
        jobsCounter=numOfFiles;
        //myID is the local's ID for the manager to use
        myID = System.currentTimeMillis();
        sqsManagerToLocal = "sqsManagerToLocal-"+myID;
        //check if theres an instance running with TAG-MANAGER AKA big bo$$
        //if there is no manager running, run a new instance of a manager, and create an SQS queueueue
        boolean isFirstLocal = AwsHelper.isOpen(AwsHelper.bucket_name);

        if(!isFirstLocal) {
            System.out.println("Manager not Online");

            AwsHelper.OpenS3();        //opens a new bucket(only if not open already), and upload manager and workers JAR files
            AwsHelper.uploadToS3("../Manager/AwsAss1.jar", "Manager.jar");
            AwsHelper.uploadToS3("../Worker/AwsAss1.jar", "Worker.jar");
            runManager();   //create a new instance of a manager
            AwsHelper.OpenSQS(sqsLocalsToManager);      //this SQS is for ALL locals to upload jobs for the manager
            //manager is now online and ready for jobs

        }
        AwsHelper.OpenSQS(sqsManagerToLocal);  //this SQS is for this local ONLY for messages about finished jobs from manager.

//        waits for the first local to finish initiating
        while(!AwsHelper.isManagerOnline())
        {try {
            TimeUnit.SECONDS.sleep(1); }catch (Exception ignored){}}

        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);

        long startTime = System.currentTimeMillis();

        for(int i=0 ; i< numOfFiles; i++) {
            String key = args[i];
            AwsHelper.uploadToS3("../Input files/"+key, key);

            //pushing job to SQS as a URL for the uploaded file
            //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]
                terminated = Integer.parseInt(args[args.length - 1])*numOfFiles; // only in the last file

            String[] arguments = {key, String.valueOf(myID), args[numOfFiles+i], args[args.length - 2],String.valueOf(terminated)};
            System.out.println("");
            for(String a : arguments)
                System.out.print(a+',');

            List<Message> list = new LinkedList<>();
            list.add(AwsHelper.toMSG(arguments));

            AwsHelper.pushSQS(sqsLocalsToManager, list);
            // manager is now able to take the job from SQS and process it. check the finished_SQS for massages of finished jobs.
        }
        //wait for all jobs to be ready at finished_SQS
        finish();

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.println("Total time took: "+ elapsedTime/(60000) + " minutes and " +(elapsedTime/(1000))%60 + " seconds");
    }



    private static void runManager() {
        System.out.println("running manager");
        if (!AwsHelper.isManagerOnline()) // for concurrency- double check
            AwsHelper.startInstance("Manager","Manager.jar");
    }


    private static void finish() throws IOException {

        // check the SQS for massages of finished jobs
        while(true){
            //Check SQS - sqsManagerToLocal for finished jobs(results)
            List<Message> finishedJobs = CheckFinishedJobs();
            List<String> finishedJobsOutputNames = new LinkedList<>();
            for (Message m: finishedJobs)
                 finishedJobsOutputNames.add(m.body());

            downloadResults(finishedJobsOutputNames); // download from s3 to one file on Local Machine

            List<Message> testlist = AwsHelper.popSQS(sqsTesting);

            //List<String> testMSG= AwsHelper.fromMSG(testlist,String.class);
            if (!testlist.isEmpty()) {
                String testMSG = testlist.get(0).body();
                System.out.println("testMSGs: " + testMSG);//.get(0)
            }
           /// if (!finishedJobs.isEmpty())
            //    System.out.println("Results sentiment:"+finishedJobs.get(0).sentiment + "\nentities: " + finishedJobs.get(0).entities);

            AwsHelper.deletefromSQS("sqsTesting",testlist);
            AwsHelper.deletefromSQS(sqsManagerToLocal,finishedJobs);

            if(jobsCounter==0)
                break;
        }
        AwsHelper.deleteSQS(sqsManagerToLocal); // delete my sqs
        System.out.println("finished");

    }

    private static void downloadResults(List<String> finishedJobsOutputNames) throws IOException {
        for (String outputname: finishedJobsOutputNames){
            //Creating the output directory
            File fileFolder = new File("./"+outputname);
            fileFolder.mkdir();
            File folder = new File("./"+outputname+"/");

            File merged = new File("./"+outputname+".html");

            PrintWriter pw = new PrintWriter(merged);
            //start writing html format to outputname.html:
            pw.println("<HTML><style>\n" +
                    "table, th, td {\n" +
                    "  border: 1px solid black;\n" +
                    "}\n" +
                    "</style><Body><table style=\"width:100%\"><tr><th>Entities</th><th>Review Link</th><th>Sarcasm Detection</th></tr>");
            int i=0;
            while(true) { // download all files and deletes them
                try {
                    AwsHelper.downloadFile(outputname + "-" + i, "./" + outputname + "/" + i );
                    AwsHelper.deleteFile(outputname + "-" + i);

                    File file = new File(folder, "./"+i);
                    BufferedReader br = new BufferedReader(new FileReader(file));
                    String line = "";
                    while ((line = br.readLine()) != null) {
                        pw.println("<tr>"+getHtmlData(line)+"</tr>");
                    }
                    i++;
                    file.delete();
                }
                catch (Exception ignored){
                    break;
                }
            }

            pw.println("</table></Body></HTML>");

            pw.flush();
            pw.close();
        }
    }

    public static void MergeTextFiles(String outputname) throws IOException {
        File f = new File("./"+outputname+"/");

        File merged = new File("./"+outputname+".html");

        PrintWriter pw = new PrintWriter(merged);

        String[] s = f.list();
        pw.println("<HTML><style>\n" +
                "table, th, td {\n" +
                "  border: 1px solid black;\n" +
                "}\n" +
                "</style><Body><table style=\"width:100%\"><tr><th>Entities</th><th>Review Link</th><th>Sarcasm Detection</th></tr>");
        for (String s1 : s) {
            File f1 = new File(f, s1);
            BufferedReader br = new BufferedReader(new FileReader(f1));

            String line = "";
            while ((line = br.readLine()) != null) {
                pw.println("<tr>"+getHtmlData(line)+"</tr>");
            }
            f1.delete();
        }
        pw.println("</table></Body></HTML>");

        pw.flush();
        pw.close();
    }

    public static String getHtmlData(String strJsonData) {
        return jsonToHtml( new JSONObject( strJsonData ) );
    }

    private static String jsonToHtml(Object obj) {
        StringBuilder html = new StringBuilder( );
        String color = "blue";
        try {
            if (obj instanceof JSONObject) {
                JSONObject jsonObject = (JSONObject)obj;
                String[] keys = JSONObject.getNames( jsonObject );

                html.append("<div class=\"json_object\">");

                if (keys.length > 0) {
                    for (String key : keys) {
                        // print the key and open a DIV


                        Object val = jsonObject.get(key);
                        // recursive call
                        if(key.startsWith("jobID")) continue;
                        if(key.startsWith("Reviewindex")) continue;

                        if(key.startsWith("sentiment"))
                        {
                            switch(val.toString()) {
                                case "0":
                                    color="darkRed";
                                    break;
                                case "1":
                                    color="red";
                                    break;
                                case "2":
                                    color="black";
                                    break;
                                case "3":
                                    color="lightGreen";
                                    break;
                                case "4":
                                    color="darkGreen";
                                    break;

                            }
                            continue;
                        }
                        html.append("<td><div>");
//                        <span class=\"json_key\">")
//                                .append(key).append("</span> : ");
                        if(val.toString().startsWith("http")) {
                            html.append( "<a href=\""+val+"\" style=\"color: "+color+"\">"  );
                            html.append( jsonToHtml( val ) );
                            html.append( "</a>" );

                        }
                        else
                          html.append( jsonToHtml( val ) );

                        // close the div
                        html.append("</div></td>");
                    }
                }

                html.append("</div>");

            } else if (obj instanceof JSONArray) {
                JSONArray array = (JSONArray)obj;
                for ( int i=0; i < array.length( ); i++) {
                    // recursive call
                    html.append( jsonToHtml( array.get(i) ) );
                }
            } else {
                // print the value
                html.append( obj );
            }
        } catch (JSONException e) { return e.getLocalizedMessage( ) ; }

        return html.toString( );
    }

    private static List<Message> CheckFinishedJobs() {
        List<Message> results = AwsHelper.popSQS(sqsManagerToLocal);
        if(results.size()>0)
            System.out.println("YAY FINISHED JOB");
        jobsCounter-= results.size();
        return results;
    }

}
