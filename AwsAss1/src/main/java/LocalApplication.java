public class LocalApplication {
    public static void main(String[] args) {
        if(!managerOnline())
            runManager();
        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);
        OpenSQSWithManajer();
        String URL = uploadToS3("bananotPATH");
        //pushing job to SQS as a URL for the uploaded file
        pushSQS(URL);
        //now the manager is able to process the job. check the SQS for massages of finished jobs.
        finish();
    }

    private static boolean managerOnline() {// by tag
        return false;
    }
    private static void runManager() {
        //open instance
        // start manager code
        // take from Amir
        // add manager tag

    }


    private static void finish() {
        // check the SQS for massages of finished jobs. (Sleep or somthing)
        //Result= checkSQS();


        //makeHtmlFile(Result);
    }

    private static void pushSQS(String url) {
    }

    private static String uploadToS3(String bananot) {
        return "URLofBananot";
    }


}
