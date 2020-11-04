# DE_Assignment_2
Group 11: The second assignment for the course Data Engineering.

0) Change the keyfile to your own key file (DO NOT POST THIS ON GITHUB)
1) Run pub.py in Pycharm (you should see it pushing files)
2) Run reader.py in Pycharm (you should see that it receives the tweets)
3) Run your pipeline
    * locally: python sentiment_pipeline.py --topic projects/dataengineering-course/topics/election_data --project dataengineering-course --subscription projects/dataengineering-course/subscriptions/election_data_sub --dataset US_election_data --table_name tweet_info
    * in VM: python3 sentiment_pipeline.py --topic projects/dataengineering-course/topics/election_data --project dataengineering-course --subscription projects/dataengineering-course/subscriptions/election_data_sub --dataset US_election_data --table_name tweet_info
    * as DataFlow: python3 sentiment_pipeline.py --runner DataflowRunner --topic projects/dataengineering-course/topics/election_data --project dataengineering-course --subscription projects/dataengineering-course/subscriptions/election_data_sub --temp_location gs://ass_2/tmp/ --job_name sentiment --region us-central1 --dataset US_election_data --table_name tweet_info
        - here we tried three different machine_types
        - our default was n1-standard-4
        - with the parameter --machine_type, we tried e2-standard-4 and n2-standard-4
        
        
4) Now your results are in BigQuery
5) You can visualize these results using the visualize.ipynb notebook.
