import java.awt.*;
import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

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
    public static void main(String[] args) throws IOException {
        System.out.println("Local Main");
        AwsHelper.OpenSQS(sqsTesting);      //TODO remove, this SQS is for ALL locals to upload jobs for the manager

        int numOfFiles = (args.length-2)/2;
        jobsCounter=numOfFiles;
        //myID is the local's ID for the manager to use
        myID = System.currentTimeMillis();
        sqsManagerToLocal = "sqsManagerToLocal-"+myID;
        //check if theres an instance running with TAG-MANAGER AKA big bo$$
        //if there is no manager running, run a new instance of a manager, and create an SQS queueueue
        if(!AwsHelper.isManagerOnline()) {
            System.out.println("Manager not Online");// todo delete

            // im the first local! :)
            AwsHelper.OpenS3();       //open a new bucket, and upload manager and workers JAR files
            AwsHelper.uploadToS3("../Manager/AwsAss1.jar","Manager.jar");
            AwsHelper.uploadToS3("../Worker/AwsAss1.jar","Worker.jar");
            runManager();   //create a new instance of a manager

            AwsHelper.OpenSQS(sqsLocalsToManager);      //this SQS is for ALL locals to upload jobs for the manager
            //manager is now online and ready for jobs
        }
        AwsHelper.OpenSQS(sqsManagerToLocal);  //this SQS is for this local ONLY for messages about finished jobs from manager.
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\ntesting sqsTesting");// todo delete


        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);
        for(int i=0 ; i< numOfFiles; i++) {
            String key = args[i];
            AwsHelper.uploadToS3("../Input files/"+key, key);

            //pushing job to SQS as a URL for the uploaded file
            //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]
                terminated = Integer.parseInt(args[args.length - 1])*numOfFiles; // only in the last file
//            String address = arguments[0];
//            String jobOwner = arguments[1];
//            String outputFileName = arguments[2];
//            n = Integer.parseInt(arguments[3]);

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
    }



    private static void runManager() {
        System.out.println("running manager");

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
            //filesToHTML(finishedJobsOutputNames); // change files from txt to HTML todo



            //TODO only for testing, remove before flight
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
        //TODO makeHtmlFile(Result);
        System.out.println("finished");

    }

    private static void downloadResults(List<String> finishedJobsOutputNames) throws IOException {
        for (String outputname: finishedJobsOutputNames){
            //Creating the output directory
            File file = new File("./"+outputname);
            file.mkdir();

            int i=0;
            while(true) { // download all files and deletes them
                try {
                    AwsHelper.downloadFile(outputname + "-" + i, "./" + outputname + "/" + i );
                    AwsHelper.deleteFile(outputname + "-" + i);
                    i++;
                }
                catch (Exception ignored){
                    break;
                }
            }
            MergeTextFiles(outputname);
        }
    }

    private static List<Message> CheckFinishedJobs() {
        List<Message> results = AwsHelper.popSQS(sqsManagerToLocal);
        if(results.size()>0)
            System.out.println("YAY FINISHED JOB");
            jobsCounter-= results.size();
        return results;
    }

    public static void MergeTextFiles(String outputname) throws IOException {
        File f = new File("./target/LocalApplication/"+outputname+"/");

        File merged = new File("./target/LocalApplication/"+outputname+".html");

        PrintWriter pw = new PrintWriter(merged);

        String[] s = f.list();
        pw.println("<HTML><style>\n" +
                "table, th, td {\n" +
                "  border: 1px solid black;\n" +
                "}\n" +
                "</style><Body><table style=\"width:100%\"><tr><th>a</th><th>b</th><th>c</th><th>d</th></tr>");
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
                        html.append("<td><div><span class=\"json_key\">")
                                .append(key).append("</span> : ");

                        Object val = jsonObject.get(key);
                        // recursive call
                        if(key.startsWith("sentiment"))
                        {
                            color=val.toString();
                        }
                        if(val.toString().startsWith("http")) {
                            html.append("<style>a:link {color: "+color
                                    +";background-color: transparent;text-decoration: none;}a:visited {color: "+color
                                    +";background-color: transparent;text-decoration: none;}a:hover {color: "+color
                                    +";background-color: transparent;text-decoration: underline;}a:active {color: "+color
                                    +";background-color: transparent;text-decoration: underline;}</style>");
                            html.append( "<a href=\""+val+"\">"  );
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

}
