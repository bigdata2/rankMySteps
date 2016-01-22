import boto

if __name__ == "__main__":
   
    total = 0

    s3 = boto.connect_s3()

    bk = s3.get_bucket('anurag-raw-data-store')

    for file_key in bk.list():
         total = total + file_key.size

    print total
