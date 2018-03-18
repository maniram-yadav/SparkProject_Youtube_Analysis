# SparkProject_Youtube_Analysis
This project has been developement on spark framework for analysis of youtube video data.

## Problem Description 

Given Youtube video data in the text file. You have to find out the answer of following question


* Listing all the category on which video has been Uploaded  **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/Extract_Video_Categories.scala)**

* Top 10 rated Youtube videos   **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/maximum_rated_video.scala)**

* Number of videos uploaded on each Category  **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/Cat_video_count.scala)**

* Number of uploaded video by each and every user **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/Uploader_Video_Count.scala)**

* Find out the Top 10 videos in each and every Category  **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/Categorywise_tpVideo.scala)**

* Finding out the list of user who have uploaded the video on single and multiple Category **[Solution](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/User_singleCategory.scala)**


## Data Description

Data has been saved in **[Youtube_Data.txt](https://github.com/maniram-yadav/SparkProject_Youtube_Analysis/blob/master/youtubedata.txt)** file. Each and every value has been seperated by tab ('\t').
The Description of the data is as follows.

**Column 1:** Video id of 11 characters.

**Column 2:** uploader of the video

**Column 3:** Interval between the day of establishment of Youtube and the date of uploading of the video.

**Column 4:** Category of the video.

**Column 5:** Length of the video.

**Column 6:** Number of views for the video.

**Column 7:** Rating on the video.

**Column 8:** Number of ratings given for the video

**Column 9:** Number of comments done on the videos.

**Column 10:** Related video ids with the uploaded video.

