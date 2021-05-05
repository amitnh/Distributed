# Assignment 1 - Distributed Sarcasm Detector

Amit Nagar Halevy 204210306
Tal Kapelnik 204117089

![image](https://user-images.githubusercontent.com/58166360/117177288-853fee00-add9-11eb-80f1-dd0499860b03.png)

<ins>Intro:</ins>

In this assignment we code a real-world application to distributively process a list of amazon reviews, perform sentiment analysis and named-entity recognition, and display the result on a web page. in the results we detect sarcasm!
The application is composed of a local application, and non-local instances running on Amazon cloud.
The application gets input text files containing lists of reviews (JSON format).
Then, instances are launched in AWS (workers & a manager) to apply sentiment analysis on the reviews and detect whether it is sarcasm or not.

<ins>How to run:</ins>

1. open the code on intellij (eclipse or any other software) and put the right AWS credentials in the "config" file. also update the keypair and securityGroup.
2. open 4 new folders: LocalAplication,Worker,Manager,InputFiles
3. make 3 jar files, all with the same name: "AwsAss1.jar" one with main from LocalAplication, another with main from Worker and the las with main from Manager.
4. put the jar files each in the corresponding folder.
5. put the json input files in the InputFiles folder.
6. run the jar in LocalApplication with this format:
![image](https://user-images.githubusercontent.com/58166360/117176480-9dfbd400-add8-11eb-85f5-134fa962793a.png)
  where n is- number of avg reviews for worker (used to decide the workers size) 
  and where terminate is- 0 or 1
 
![image](https://user-images.githubusercontent.com/58166360/117179538-e49efd80-addb-11eb-80e5-9aaf622883d5.png)
![image](https://user-images.githubusercontent.com/58166360/117179573-eff22900-addb-11eb-9a24-d90d21cea521.png)

how we chose to implement this:
![image](https://user-images.githubusercontent.com/58166360/117188173-6d6e6700-ade5-11eb-8262-a9ddc6d647ff.png)

this way we can work in parallel.

----------------------------------------------------------------------------------------------
<ins>Threads:</ins>

Worker- uses a Thread pool.

Manager- uses a Thread pool, and have 2 main tasks:

1. Receive messages from Local Machines.
2. Upload results from SqsResult to S3

A thread finished a task he pushes the other task to the Thead pool.
for example- if tread finished job 1, he puts in the Thead pool task 2.
this way the manager can work in parallel, but also distribute his power and threads in a smart way.

----------------------------------------------------------------------------------------------
<ins>Scalability:</ins>

-Each worker uses the same SQS and pull only one message, so if we add workers the system will performe better.
-We can use multiple LocalApllications there is no limit.
-We use all the threads available by the instances we use.

----------------------------------------------------------------------------------------------
<ins>Persistence:</ins>

The worker only deletes the review after he finished processing it.
so if he didn't finished for some reson, another worker will take this review for processing.

----------------------------------------------------------------------------------------------
<ins>Several Clients At The Same Time:</ins>

we are fullt supports a run of multiple clients.
Only when a client with terminate = 1, finished all his job -> all workers and Manager will terminate

----------------------------------------------------------------------------------------------
<ins>System Limitations:</ins>
the main limitation is the sqs in-flight max size of 120,000 messages.
thats means onlt 120,000 workers can work simultaneously. thats a lot, but it still limited.
Our program supports, adding up different sizes of reviews in a single message.
so if we put even only 2 reviews in the same message we can increase the number to 240,000 workers.

Another limitation is the message size, sqs only supports message up to 2GB. its a very big number but also limited.
So a big review or a batch of review is limited.
a good solution will be to upload the reviews to S3 first. and then only sending links as messages for the workers. 

----------------------------------------------------------------------------------------------
type of instance we used:

ami:0009c3f63fca71e34 linux with java8 
type: T2_XLARGE- but it also works with medium, we used XL for the large amount of threads.
The Program took 16:15 minutes to run with n=500.
*we get the Results on S3-bucket in only a few minutes, the buttle neck is downloading them on the Local Machine.
if we could choose to make the HTML file on the AWS servers, we could do it much much faste, for example with Map-Reduce learned in class.*

 
 
