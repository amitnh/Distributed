# Assignment 1 - Distributed Sarcasm Detector

Amit Nagar Halevy 204210306
Tal Kapelnik 

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


----------------------------------------------------------------------------------------------
<ins>Scalability:</ins>

----------------------------------------------------------------------------------------------
<ins>persistence:</ins>

The worker only deletes the review after he finished processing it.
so if he didn't finished for some reson, another worker will take this review for processing.

----------------------------------------------------------------------------------------------
<ins>SQS:</ins>

SQS queue on AWS doesn't promise us that we couldn't get duplicated messages, therefore we gave each review and each Job a uniqe Id.

----------------------------------------------------------------------------------------------
<ins>Threads:</ins>

----------------------------------------------------------------------------------------------
<ins>Several Clients At The Same Time:</ins>

----------------------------------------------------------------------------------------------
<ins>System Limitations:</ins>

120,000 but we can decrease the batch size

----------------------------------------------------------------------------------------------

type of instance you used (ami + type:micro/small/large…),
how much time it took your program to finish working on the input files,
and what was the n you used.

 
 
