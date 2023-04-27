from arcticdb import Arctic
ac = Arctic('s3://arn:aws:s3:ca-central-1:825739349130:accesspoint/actest')
ac.create_library('data')
#*

ac.list_libraries()
