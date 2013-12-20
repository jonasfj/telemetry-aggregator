from boto.s3 import connect_to_region as s3_connect
from boto.s3.key import Key
from utils import mkdirp
from shutil import rmtree
from glob import glob
import os, sys, gzip
from datetime import datetime
from updateresults import updateresults
from traceback import print_exc


aws_cred = {
    'aws_access_key_id':        None,
    'aws_secret_access_key':    None
}

work_folder = "/mnt/work/work-folder/"
publish_prefix = "v6/"
publish_bucket_name = "telemetry-dashboard"

s3 = s3_connect('us-west-2')
publish_bucket = s3.get_bucket(publish_bucket_name)
input_bucket = s3.get_bucket('dashboard-mango-aggregates')

# Clear work-folder
rmtree(work_folder, ignore_errors = True)

cache_folder = os.path.join(work_folder, "cache")
data_folder = os.path.join(work_folder, "data")
temp_folder = os.path.join(work_folder, "temp")
mkdirp(data_folder)
mkdirp(temp_folder)
mkdirp(cache_folder)

inputs = []
for p in input_bucket.list(prefix = 'merged/', delimiter = '/'):
    inputs.append(p.name)

def get_input(prefix, path):
  k = Key(input_bucket)
  k.key = prefix
  retries = 0
  while retries < 10:
    try:
      k.get_contents_to_filename(path)
      break
    except:
      retries += 1
      if retries > 10:
        print sys.stderr, "Failed to get " + prefix
        raise

parts_path = os.path.join(temp_folder, 'parts')
result_path = os.path.join(temp_folder, 'result.txt.lz4')

def do(cmd):
  retval = os.system(cmd)
  if retval != 0:
    print >> sys.stderr, "Command failed: " + cmd
    raise Exception("Command failed: " + cmd)


# Get data from temp_folder to data_folder
def handle_input():
  if os.path.getsize(result_path) < 1024 * 1024 * 1024:
    print >> sys.stderr, "File is too little, I assume broken!"
    return False
  unpacked_prefix = os.path.join(temp_folder, 'result.txt-')
  try:
    do('../lz4/lz4 -d %s | split -d -l 5000 - %s' % (result_path, unpacked_prefix))
  except:
    return False
  unpacked = glob(unpacked_prefix + '*')
  if len(unpacked) < 2:
    print >> sys.stderr, "There should be more than 2 files!"
    return False
  unpacked.sort()
  for f in unpacked:
    print "doing: " + f
    do('./build/mergeresults -f %s -i %s' % (data_folder, f))
  return True


# publication stuff
files_missing_path = os.path.join(work_folder, 'FILES_MISSING')
files_processed_path = os.path.join(work_folder, 'FILES_PROCESSED')

def get_file(prefix, filename):
  try:
    k = Key(publish_bucket)
    k.key = publish_prefix + prefix
    k.get_contents_to_filename(filename)
  except:
    print >> sys.stderr, "Failed to get: " + filename
    os.system('touch ' + filename)

def put_file(filename, prefix):
  k = Key(publish_bucket)
  k.key = publish_prefix + prefix
  k.set_contents_from_filename(filename)

# Get FILES_
get_file('FILES_PROCESSED', files_processed_path)
get_file('FILES_MISSING', files_missing_path)


missing = []
processed = []
count = 0

for prefix in inputs:
  rmtree(temp_folder, ignore_errors = True)
  mkdirp(temp_folder)
  started = datetime.utcnow()
  print "### Starting %s" % prefix
  try:
    get_input(prefix + 'parts', parts_path)
    #get_input(prefix + 'result.txt.lz4', result_path)
    do("aws s3 cp s3://dashboard-mango-aggregates/" + prefix + 'result.txt.lz4 ' + result_path + " --region 'us-west-2'")
    print "Downloaded %s in %s s" % (prefix, (datetime.utcnow() - started).seconds)
    if handle_input():
      print "Handled input in %s s" % ((datetime.utcnow() - started).seconds)
      with open(parts_path, 'r') as parts:
        for p in parts:
          processed.append(p.strip())
    else:
      print "Input failed in %s s" % ((datetime.utcnow() - started).seconds)
      with open(parts_path, 'r') as parts:
        for p in parts:
          missing.append(p.strip())
    count += 1
    if count > 4:
      print "Publishing results"
      # Create work folder for update process
      update_folder = os.path.join(work_folder, "update")
      shutil.rmtree(update_folder, ignore_errors = True)
      mkdirp(update_folder)
      updateresults(data_folder, update_folder, publish_bucket_name,
                    publish_prefix, cache_folder, 'us-west-2',
                    aws_cred, 16)
      # Update processed files
      with open(files_processed_path, 'a+') as files_processed:
        for p in processed:
          files_processed.write(p + '\n')
      with open(files_missing_path, 'a+') as files_missing:
        for m in missing:
          files_missing.write(m + "\n")
      # Put meta files
      put_file(files_processed_path, 'FILES_PROCESSED')
      put_file(files_missing_path, 'FILES_MISSING')
      # Clear data folder
      rmtree(data_folder, ignore_errors = True)
      mkdirp(data_folder)
      count = 0
  except:
    print >> sys.stderr, "UNHANDLED ERROR: Aborting parts so far, due to:"
    print_exc(file = sys.stderr)
    # Okay, everything since last publication failed
    count = 0
    missing += processed
    processed = []
    # Put files missing
    with open(files_missing_path, 'a+') as files_missing:
      for m in missing:
        files_missing.write(m + "\n")
    put_file(files_missing_path, 'FILES_MISSING')
    # Clear data folder
    rmtree(data_folder, ignore_errors = True)
    mkdirp(data_folder)


if __name__ == "__main__":
    sys.exit(main())