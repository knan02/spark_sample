import os
import time
import argparse
import subprocess
import json
import yaml

def run_cmd(cmd):
    try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        output, err = p.communicate()
    except (ValueError, OSError, subprocess.CalledProcessError) as err:
        raise Exception(err)
    return (output, err)


def copy_from_local(s3path, TEMPFILEPATH):
    print "copying from file:{0} to s3 :{1}".format(TEMPFILEPATH, s3path)
    cmd = "aws s3 cp {0} {1}".format(TEMPFILEPATH, s3path)
    print cmd
    output, err = run_cmd(cmd)
    if err:
        raise Exception('err while copying files from local to s3...{0}'.format(err))
        return None
    return output


def is_step_running_step(step_id, cluster_id, region):
    cmd = "aws emr describe-step --cluster-id {} --step-id {} --region {}".format(
        cluster_id,
        step_id,
        region
    )

    output, err = run_cmd(cmd)

    if not err:
        try:
            json_data = json.loads(output)
            print 'json_data: ', json_data
            job_state = json_data['Step']['Status']['State']
            return job_state == 'PENDING' or job_state == 'RUNNING'
        except Exception as err:
            raise Exception('err while parsing output from describe step... {0}'.format(err))
    raise Exception('err while checking if step is still running... {0}'.format(err))

'''
def create_step(arg, dirname):
    input_str = " ".join(arg['input_list'])
    output_str = " ".join(arg['output_list'])
    aws_step_cmd_tmpl = 'aws emr add-steps --cluster-id {0} --region {1} --steps Type=CUSTOM_JAR,Name=sparkCustomJar,ActionOnFailure=CONTINUE,Jar=s3://{2}.elasticmapreduce/libs/script-runner/script-runner.jar,Args=["{3}"]'
    aws_step_cmd = aws_step_cmd_tmpl.format(arg['cluster_params']['cluster_id'],
                                            arg["cluster_params"]["region"],
                                            arg["cluster_params"]["region"],
                                            dirname)
    print aws_step_cmd
    return aws_step_cmd
'''

def copy_file_to_s3_path(local_file_path, aws_s3_path):
    cmd = "aws s3 cp {0} {1}".format(local_file_path, aws_s3_path)
    print cmd
    output, err = run_cmd(cmd)
    if err:
        raise Exception('err while copying files from local to s3...{0}'.format(err))
    return output

def is_step_running(step_id, cluster_id, region):
    cmd = "aws emr describe-step --cluster-id {} --step-id {} --region {}".format(
        cluster_id,
        step_id,
        region
    )

    output, err = run_cmd(cmd)

    if not err:
        try:
            json_data = json.loads(output)
            print 'json_data: ', json_data
            job_state = json_data['Step']['Status']['State']
            return job_state == 'PENDING' or job_state == 'RUNNING'
        except Exception as err:
            raise Exception('err while parsing output from describe step... {0}'.format(err))
    raise Exception('err while checking if step is still running... {0}'.format(err))

def generate_spark_custom_script(args_json):
    tmp_file_path = "/tmp/custom_spark_script_{}.sh".format(args_json['exec_job_id'])
    file = open(tmp_file_path, "w")
    kuyil_foder = "kuyil_exec_job_{}".format(args_json['exec_job_id'])
    file.write('cd $HOME\n')
    file.write("mkdir -p {}\n".format(kuyil_foder))
    file.write("cd {}\n".format(kuyil_foder))
    s3_dependencies_path = "s3://4info-test/kuyil/uploads/kuyil_exec_job_{}".format(args_json['exec_job_id'])
    file.write("aws s3 cp {} . --recursive \n".format(s3_dependencies_path))
    file.write("{}\n".format(args_json['spark_cmd']))

    file.close()
    return tmp_file_path



if __name__ == "__main__":

	parser = argparse.ArgumentParser()
	parser.add_argument('--args', action='store', dest='args', type=str, help='Store')
	known_args, unknown_args = parser.parse_known_args()
	if len(unknown_args) > 0:
	    raise Exception('unknown args {0}'.format(unknown_args))

	known_args_dict = vars(known_args)
	args_json = yaml.safe_load(known_args_dict['args'])

	tmp_custom_script_path = generate_spark_custom_script(args_json)
	aws_custom_script_path = "s3://4info-test/kuyil/system_generated_custom_scripts/"
	copy_file_to_s3_path(tmp_custom_script_path, aws_custom_script_path)
	aws_step_cmd_tmpl = 'aws emr add-steps --cluster-id {0} --region "{1}" --steps Type=CUSTOM_JAR,' \
		            'Jar=s3://elasticmapreduce/libs/script-runner/script-runner.jar,Name="{2}",' \
		            'ActionOnFailure=CONTINUE,Args=[{3}]'

	cluster_params = args_json['cluster_params']

	aws_step_cmd = aws_step_cmd_tmpl.format(
	    cluster_params['cluster_id'],
	    cluster_params['region'],
	    args_json['job_name'],
	    "{}{}".format(aws_custom_script_path, os.path.basename(tmp_custom_script_path))
	)

	print aws_step_cmd

	exec_output, exec_err = run_cmd(aws_step_cmd)
	print exec_err

	try:
	    if not exec_err:
		json_exec_output = json.loads(exec_output)
		step_ids = json_exec_output["StepIds"]
		print "step id: {}".format(step_ids)

		is_still_running = True
		while (is_still_running):
		    time.sleep(15)
		    print 'checking step...'
		    is_still_running = is_step_running(
		        step_ids[0],
		        cluster_params['cluster_id'],
		        cluster_params['region'],
		    )
	    else:
		raise Exception('error while executing step on cluster - {0}'.format(exec_err))
	except Exception as err:
	    raise Exception('execution interrupted in hive wrapper - {0}'.format(err))




