# Run MapReduce Google Cloude in order to mine tweets   

Create Project enable billing and Google Cloud APIs  

### Add API:  
•	Compute Engine API  
•	Cloud BigTable API  
•	Cloud BigTable Table Admin API  
•	Google Cloud DataProc API  
•	Cloud BigTable Admin API  

### Create a VM instance  
•	GCP menu-> Computing engine->VM instance->create  
•	Choose zone same one as before: us-east1-b  
•	Choose Machine type: 2vCPUs.  
•	Choose boot disk: ubuntu 16.04, boot disk type SSD disk  
•	in Identity and API access: Allow full access to all Cloud APIs  
•	In Firewall: allow both access, HTTP and HTTPs  

### Add IMA roles to youre user:  
•	Project Billing Manager  
•	BigTable Administrator  
•	Compute Admin  
•	DataProc Editor  
•	Project Owner  
•	Storage Admin  
•	Logging Admin  

### Create a big table instance:  
•	Choose indtance name: for example - sharon-mapreduce-bigtable  
•	Write down the Instance id: for example - sharon-mapreduce-bigtable  
•	In Instance type: Choode Development  
•	In Storage type: Choose SSD  
•	Zone: us-east1-b  

### Open VM Instance SSH

Initialize Google Cloud
```
gcloud init 
```

install java:
```
sudo apt-get update
sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
sudo apt-get update
sudo apt-get install oracle-java8-set-default
```

set up JAVA_HOME var:
```
export JAVA_HOME=/usr/lib/jvm/java-8-oracle
sudo apt-get update -y
```

Install Maven and Hadoop
```
sudo apt-get install maven
cd
wget www-eu.apache.org/dist/hadoop/common/hadoop-3.1.0/hadoop-3.1.0.tar.gz
tar xzf hadoop-3.1.0.tar.gz
cd
mv hadoop-3.1.0 hadoop
export PATH="$PATH:$HOME/hadoop/bin"
```

Install python3/pip3:
```
sudo apt-get update
sudo apt-get -y install python3-pip
```

Install python packages needed for working with BigTable:
```
pip3 install google-cloud-storage
pip3 install google-cloud-happybase
pip3 install google-cloud-dataproc
```

Create Project storage:
```
gsutil mb -p <project ID> gs://<bucketName>
# For example
gsutil mb -p sharon-project-204821 gs://sharon-bucket
```

Make your Buckets pablic:
```
gsutil defacl set public-read gs://sharon-bucket
```

Clone and Configure MapRduce example:
```
git clone https://github.com/sharon-hadar-leverate/gcloud_mapreduce.git
cd gcloud_mapreduce/
#mvn clean package -Dbigtable.projectID=YOUR_PROJECT_ID -Dbigtable.instanceID=YOUR_INSTANCE_ID
mvn clean package -Dbigtable.projectID=sharon-project-204821	 -Dbigtable.instanceID=sharon-mapreduce-bigtable
```

Create Google Cloud DataProc cluster
```	
gcloud dataproc clusters create "sharon-mapreduce-bigtable" \
    --bucket "sharon-bucket" \
    --num-workers 2 \
    --zone us-east1-c \
    --master-machine-type n1-standard-2 \
    --worker-machine-type n1-standard-2
```
	
Run MapReduce word count BigTable DataProc (example)
```	
gcloud dataproc jobs submit hadoop --cluster sharon-mapreduce-bigtable \
    --jar target/wordcount-mapreduce-1.0-jar-with-dependencies.jar \
    -- wordcount-hbase \
    gs://lesv-big-public-data/books/book \
    gs://lesv-big-public-data/books/b6130 \
    "words-count"
```
```
./cluster.sh start  sharon-mapreduce-bigtable 
```

#### Run code
you need to pip3 install:
```
pip3 install numpy
pip3 install pandas
pip3 install bs4
pip3 install tweepy
pip3 install nltk
pip3 install geopy
```

Now you can run find_top_words.py:
```
find_top_words.py -q "query to search" -n "location"
```





