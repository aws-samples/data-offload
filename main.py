# Sample code, software libraries, command line tools, proofs of concept, templates, or other related technology are provided as AWS Content or Third-Party 
# Content under the AWS Customer Agreement, or the relevant written agreement between you and AWS (whichever applies). You should not use this AWS Content or 
# Third-Party Content in your production accounts, or on production or other critical data. You are responsible for testing, securing, and optimizing the AWS 
# Content or Third-Party Content, such as sample code, as appropriate for production grade use based on your specific quality control practices and standards. 
# Deploying AWS Content or Third-Party Content may incur AWS charges for creating or using AWS chargeable resources, such as running Amazon EC2 instances or # 
# using Amazon S3 storage.”

import os, subprocess, argparse, json, numpy, sys
from datetime import datetime
import logging
from multiprocessing import Process
import time
import boto3
from botocore.exceptions import ClientError

def get_source_manifest(source):
    source_file_dict = {}
    local_files = os.listdir(source)
    for file_name in local_files:
        file_path = os.path.join(source, file_name)

        # Get size and creation time of local file
        local_size = os.path.getsize(file_path)
        local_time = datetime.utcfromtimestamp(os.path.getctime(file_path))
        source_file_dict[file_name] = {}
        source_file_dict[file_name]['size'] = local_size
        source_file_dict[file_name]['local_time'] = local_time
    
    return source_file_dict

def get_dest_manifest(destination):
    s3_files_dict = {}
    for id,snowball in enumerate(destination['snowballs']):
        s3_files = []
        cur_snowball_files = []
        try:
            s3_files_output = subprocess.check_output(
                ['s5cmd', '--log', 'debug', '--endpoint-url', destination["snowballs"][id]["endpoint"],
                '--use-list-objects-v1',
                '--profile', destination["snowballs"][0]["profile"], 'ls', f's3://{destination["snowballs"][id]["bucket"]}/'], stderr=subprocess.STDOUT).decode('utf-8')
        except subprocess.CalledProcessError as e:
            logging.debug("s5cmd process returned error " + str(e.output))
            s3_files_output = ""
        cur_snowball_files = s3_files_output.split('\n')
        #remove empty strings
        cur_snowball_files = [i for i in cur_snowball_files if i]
        for x in cur_snowball_files:
            s3_files.append(x)

        for line in s3_files:
            parts = line.split()
            if len(parts) < 4:
                continue
            size = int(parts[2])
            time = datetime.strptime(' '.join(parts[0:2]), '%Y/%m/%d %H:%M:%S')
            name = parts[3]
            s3_files_dict[name] = {}
            s3_files_dict[name]['size'] = size
            s3_files_dict[name]['local_time'] = time
        
        if destination['type'] == "s3compatible":
            break    
    return s3_files_dict

def compare_source_dest(source, destination):
    delta_files_dict = {}
    for file in source.keys():
        if file not in destination.keys():
            delta_files_dict[file] = source[file]
            continue
        if file in destination.keys() and source[file]['size'] != destination[file]['size']:
            delta_files_dict[file] = source[file]
    return delta_files_dict

def build_commands(source_path, destination, configfile, run_time):
    
    num_snowballs = len(destination['snowballs'])
    file_split = numpy.array_split(list(destination['copylist'].keys()), num_snowballs)

    for i in range(0, num_snowballs):
        pending_commands = []
        pending_command_file_name = 'logs/' + str(configfile) + '_' + run_time + '_commands_pending_' + str(destination['snowballs'][i]['name']) + '.txt'
        for file in file_split[i]:
            file_path = os.path.join(source_path, file)

            #pending_commands.append(f'cp "{file_path}" s3://{destination["bucket"]}/{file_name.replace(" ","")}')
            pending_commands.append(f'cp "{file_path}" s3://{destination["snowballs"][i]["bucket"]}/{file.replace(" ","")}')
        with open(pending_command_file_name, 'w') as f:
            for line in pending_commands:
                f.write("%s\n" % line)
        logging.debug("Wrote " + str(len(pending_commands)) + " commands to file " + str(pending_command_file_name))

    return pending_command_file_name

def run_s3_commands(numworkers, log_level, endpoint, snowball_profile, command, stdoutfile, stderrfile, config):
    subproc = subprocess.Popen(
            ['s5cmd', '--numworkers', numworkers,  '--stat', '--log', log_level, '--endpoint-url',
             endpoint, '--profile', snowball_profile,
             'run', command],
            stdout=stdoutfile,
            stderr=stderrfile
        )
    subproc.wait()
    if subproc.returncode == 0:
        return True
    else:
        return False


def format_time(seconds):
    if seconds < 60:
        return time.strftime("%S Sec", time.gmtime(seconds))
    if seconds >= 60 and seconds < 3600:
        return time.strftime("%M Min %S Sec", time.gmtime(seconds))
    if seconds >= 3600:
        return time.strftime("%H Hr %M Min %S Sec", time.gmtime(seconds))

def check_status(dataimport, cluster, pending_dataimport_file_count,orig_completed_dataimport_file_count,pending_cluster_file_count, orig_completed_cluster_file_count, start_time):

    while True:
        time.sleep(60)
        cluster_s3_files_count = len(get_s3_file_list(cluster))
        dataimport_s3_files_count = len(get_s3_file_list(dataimport))

        elapsed_time = time.time() - start_time
        logging.info("Time Elapsed: " + str(format_time(elapsed_time)))

        total_pending_file_count = pending_cluster_file_count + pending_dataimport_file_count
        files_transferred = cluster_s3_files_count + dataimport_s3_files_count - orig_completed_dataimport_file_count - orig_completed_cluster_file_count
        logging.info("Files Transferred: " + str(files_transferred) + " / " + str(total_pending_file_count))

        if files_transferred > 0:
            logging.info("Transfer Speed: " + str(round(files_transferred/elapsed_time,2)) + " files per second")

            remaining_files = total_pending_file_count - files_transferred
            time_remaining =  remaining_files / (files_transferred/elapsed_time)
            logging.info("Estimated Time Remaining: " + format_time(time_remaining) + "\n")


def setuplogging(log_level, config_file, run_time):
    numeric_level = getattr(logging, log_level.upper(), None)
    if not os.path.exists('logs'):
        os.makedirs('logs')
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % log_level)
    logging.basicConfig(level=numeric_level, filename='logs/' + config_file + '_' + run_time + '.log', format='%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %H:%M:%S ')
    consoleHandler = logging.StreamHandler()
    logFormatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s', datefmt='%m/%d/%Y %H:%M:%S ')
    consoleHandler.setFormatter(logFormatter)
    logging.getLogger().addHandler(consoleHandler)
    
    logging.info("##############################\n") 
    logging.info("Read config successful")

def get_line_count(file):
    with open(file,'r') as file:
        cnt = 0
        for line in file: 
            cnt += 1
    return cnt

def validate_config(config):
    
    for group in list(config['destinations'].keys()):
        snowball_list = []
        bucket_list = []
        for snowball in config['destinations'][group]['snowballs']:
            snowball_list.append(snowball['name'])
            bucket_list.append(snowball['bucket'])
        # make sure there aren't any duplicate names in snowball names
        if len(snowball_list) != len(set(snowball_list)):
            logging.info("Exiting becuase the group named " + group + " in the specified config file has a duplicate name")
            sys.exit()

        # make sure that s3compatible groups have the same bucket name
        if config['destinations'][group]['type'] == 's3compatible' and len(set(bucket_list)) != 1:
            logging.info("Exiting becuase the group named " + group + " is s3compatible type and doesn't have the same bucket name for all the snowballs in the group")
            sys.exit()

        for snowball in config['destinations'][group]['snowballs']:
          session = boto3.Session(profile_name=snowball['profile'])
          s3 = session.client('s3', endpoint_url=snowball['endpoint'])
          try:
            response = s3.list_buckets()
          except ClientError as e:
            #logging.error(e)
            logging.info("AWS profile named " + snowball['profile'] + "is unable to list buckets.  Check your AWS CLI profile's credentials, region and endpoint url")
            sys.exit()
        
        for snowball in config['destinations'][group]['snowballs']:
          session = boto3.Session(profile_name=snowball['profile'])
          s3 = session.client('s3', endpoint_url=snowball['endpoint'])
          try:
            response = s3.head_bucket(Bucket=snowball['bucket'])
          except ClientError as e:
            logging.error(e)
            logging.info("Bucket " + snowball['bucket'] + " is not accessible with the AWS profile " + snowball['profile'])
            sys.exit()

def report_status(group, source_manifest, destination_manifest, run_time, config_file, config):
  remaining_manifest = compare_source_dest(source_manifest, destination_manifest)

  source_size=0
  #add up all file sizes
  for source_file in source_manifest.keys():
    source_size += source_manifest[source_file]['size']
  #convert to GB
  source_size=source_size/1000000000
  
  #repeat for remaining files
  remaining_size=0
  for remaining_file in remaining_manifest.keys():
    remaining_size += remaining_manifest[remaining_file]['size']
  if remaining_size != 0:
    remaining_size = remaining_size/1000000000

  errors = 0
  successes = 0
  totaltransfers = 0
  for snowball in config['destinations'][group]['snowballs']:
    try:
      with open('logs/' + config_file + '_' + run_time + '_stdout_' + snowball['name'] + '.txt') as file:
        getnextline = False
        for line in file:
          if getnextline:
            status = line.split()
            errors += int(status[2])
            successes += int(status[3])
            totaltransfers = int(status[1])
            break
          if line.startswith("Operation"):
            getnextline = True
    except:
      pass
  #print out status bar and description
  print_progress_bar(((source_size-remaining_size)/source_size)*100, 100, group)
  logging.info("[" + group + "] " + str(round(source_size-remaining_size,1)) + "GB of " + str(round(source_size,1)) + "GB copied. " + str(round(remaining_size,1))  +  "GB remaining")
  logging.info(str(errors) + " out of " + str(totaltransfers) + " transfers failed")
  print("\n")

def print_progress_bar(index, total, label):
    n_bar = 50  # Progress bar width
    progress = index / total
    sys.stdout.write(f"[{'=' * int(n_bar * progress):{n_bar}s}] {int(100 * progress)}%  {label}\n")
    sys.stdout.flush()

def main():    
    #Get arguments
    parser = argparse.ArgumentParser(description='Snowball data offload')
    parser.add_argument('--config_file', required=True, help='config file to use for this job, use format of config.json in this repository')
    args = parser.parse_args()

    # Open Config file
    if not os.path.isfile(args.config_file):
        print("Config file specified does not exist. Exiting...")
        sys.exit()
    with open(args.config_file, "r") as jsonfile:
        config = json.load(jsonfile)

    run_time = datetime.now().strftime('%H%M%S_%d%m%Y')
    
    log_level = config["log_level"]
    numworkers = config["num_workers"]
    
    # Enable logging
    setuplogging(log_level, args.config_file, run_time)

    # Validate config.json format
    validate_config(config)
    
    # capture start time
    start_time = time.time()

    # Keep track of running processes
    procs = []

    source_file_dict = get_source_manifest(config["source"])
    destination_groups = list(config['destinations'].keys())

    # Run offload processes for each group
    for id, group in enumerate(destination_groups):
        #get list of files from destination group
        dest_file_dict = get_dest_manifest(config['destinations'][group])

        #compare source and destination
        logging.info("Comparing files in directory " + config["source"] + " to files on destination group " + str(destination_groups[id]))
        config['destinations'][group]['copylist'] = compare_source_dest(source_file_dict, dest_file_dict)
        completed_files = len(source_file_dict.keys()) - len(config['destinations'][group]['copylist'])
        logging.info(f"Found {len(config['destinations'][group]['copylist'])} pending files and {completed_files} completed files")

        if len(config['destinations'][group]['copylist']) > 0:
            # Write out copy commands for s5cmd to use
            copy_commands = build_commands(config['source'], config['destinations'][group],args.config_file, run_time)

            # Launch offload processes for each snowball in this group
            num_snowballs = len(config['destinations'][group]['snowballs'])
            for i in range(0, num_snowballs):
                pending_command_file_name = 'logs/' + str(args.config_file) + '_' + run_time + '_commands_pending_' + str(config['destinations'][group]['snowballs'][i]['name']) + '.txt'
                stdout_file = open('logs/' + args.config_file + '_' + run_time + '_stdout_' + str(config['destinations'][group]['snowballs'][i]['name']) + '.txt', "w")
                stderr_file = open('logs/' + args.config_file + '_' + run_time + '_stderr_' + str(config['destinations'][group]['snowballs'][i]['name']) + '.txt', "w")
                pending_command_count = get_line_count(pending_command_file_name)

                if pending_command_count != 0:
                    # Create a process for s5cmd
                    proc = Process(target=run_s3_commands, args=(numworkers, log_level, config['destinations'][group]['snowballs'][i]["endpoint"], config['destinations'][group]['snowballs'][i]["profile"], pending_command_file_name, stdout_file, stderr_file,config,))
                    procs.append(proc)
                    proc.start()
                    logging.info("Launched offload process for snowball at " + config['destinations'][group]['snowballs'][i]["endpoint"])
            logging.info("Finished launching offload processes for group named " + group)
            logging.info("\n")
    # Monitor offload processes
    if len(procs) > 0:
        logging.info("Waiting for data offload processes to finish ...\n")
        
        while True:
          for proc in procs:
            #restart while loop if a process is alive
            if proc.is_alive(): break
          else:
            #if no processes are alive, break out of while
            break
          
          #get files up front to eliminate flickering/lag
          for id, group in enumerate(destination_groups):
            config['destinations'][group]['dest_manifest']=get_dest_manifest(config['destinations'][group])

          print("\033[H\033[J", end="")
          #print("Current Status:")
          logging.info("Checking status")
          for id, group in enumerate(destination_groups):
            report_status(group, source_file_dict, config['destinations'][group]['dest_manifest'], run_time,args.config_file, config)
          time.sleep(config["reporting_frequency"])


        print("\033[H\033[J", end="") 
        for id, group in enumerate(destination_groups):
          config['destinations'][group]['dest_manifest']=get_dest_manifest(config['destinations'][group])
        for id, group in enumerate(destination_groups):
          report_status(group, source_file_dict, config['destinations'][group]['dest_manifest'], run_time,args.config_file, config)
        logging.info("All data offload processes are finished ...\n")

    return

main()
