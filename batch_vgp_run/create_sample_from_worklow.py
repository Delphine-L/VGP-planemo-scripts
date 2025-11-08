#!/usr/bin/env python3

import json
import argparse
import textwrap
from bioblend.galaxy import GalaxyInstance


def main():

	parser = argparse.ArgumentParser(
						prog='create_sample_from_worklow.py',
						description='Create an empty job file from a workflow file',
						usage='create_sample_from_worklow.py -w <Workflow file> -o <Output yaml file>  ',
						formatter_class=argparse.RawTextHelpFormatter,
						epilog=textwrap.dedent('''
											General outputs: 
											- <Output yaml file>: Template Yaml file for the workflow
											'''))

	parser.add_argument('-w', '--workflow',required=False, help='The galaxy workflow file (.ga)')  
	parser.add_argument('-o', '--output_yaml', required=True, help='The output yaml file')  
	group = parser.add_argument_group("From Invocation","Use the following options to use a workfow invocation to create a job file.")
	group.add_argument('--from_invocation', action='store_true', required=False, help='Add new species to the table')
	group.add_argument('-g','--galaxy_url',  required=False, help='Galaxy Url')
	group.add_argument('-k','--APIkey', required=False, help='API key for this Galaxy instance')
	group.add_argument('-i','--invocation', required=False, help='Invocation ID')
	args = parser.parse_args()

	if args.from_invocation:
		if args.galaxy_url==False:
			raise SystemExit("Missing option: -g. If you select the --from_invocation option, you need to provide a Galaxy URL.") 
		elif args.APIkey==False:
			raise SystemExit("Missing option: -k. If you select the --from_invocation option, you need to provide an API Key.") 
		elif args.invocation==False:
			raise SystemExit("Missing option: -i. If you select the --from_invocation option, you need to provide an invocation number.") 

		gi = GalaxyInstance(args.galaxy_url, args.APIkey)
		last_run=gi.invocations.show_invocation(args.invocation)
		wfl_id=last_run['workflow_id']
		workflow_data=gi.workflows.show_workflow(wfl_id, instance=True)
		steps=last_run['steps']
		input_step_ids=[ value['workflow_step_id'] for key, value in last_run['inputs'].items()]

		dic_inputs={}
		for i in input_step_ids:
			input_label=[inp['workflow_step_label'] for inp in steps if inp['workflow_step_id']==i][0]
			input_id=[inp['id'] for inp in steps if inp['workflow_step_id']==i][0]
			dic_inputs[input_label]=input_id

		inputs_invocation=last_run['inputs']
		input_steps={}
		for key, value in inputs_invocation.items():
			input_steps[value['workflow_step_id' ]]=value['id' ]

		dict_input_data={}
		for step_id in input_steps.keys():
			label_input= [i['workflow_step_label'] for i in steps if i['workflow_step_id']==step_id][0]
			dataset_id=input_steps[step_id]
			dict_input_data[label_input]=dataset_id
			
		inputs_dic={key: value for key, value in workflow_data['steps'].items() if key in workflow_data['inputs'].keys()}

		dic_parameters={}
		inputs_invocation=last_run['inputs']

		for i in inputs_dic.keys():
			name_param=workflow_data['inputs'][i]['label']
			input_type=inputs_dic[i]['type']
			tool_state= inputs_dic[i]['tool_inputs']
			if input_type=="data_collection_input":
				collection_type=tool_state['collection_type']
				input_id=dict_input_data[name_param]
				def_value='\n  class: Collection\n  collection_type: '+collection_type+'\n  galaxy_id: '+input_id 
			elif input_type=="data_input":
				input_id=dict_input_data[name_param]
				def_value='\n  class: File\n  galaxy_id: '+input_id
			elif input_type=="parameter_input":
				param_type=tool_state['parameter_type']
				def_value=last_run['input_step_parameters'][name_param]['parameter_value']
			dic_parameters[name_param]=def_value

	else: 
		if args.workflow==False:
			raise SystemExit("Missing option: -w. If you don't select the --from_invocation option, you need to provide a workflow file.") 
		
		wf1json=open(args.workflow)
		reswf1=json.load(wf1json)
		inputs_dic={key: value for key, value in reswf1['steps'].items() if 'Input' in value['name']}

		dic_parameters={}

		for i in inputs_dic.keys():
			name_param=inputs_dic[i]['label']
			input_type=inputs_dic[i]['type']
			tool_state= json.loads(inputs_dic[i]['tool_state'])
			if input_type=="data_collection_input":
				collection_type=tool_state['collection_type']
				def_value='\n  class: Collection\n  collection_type: '+collection_type+'\n  elements:'
			elif input_type=="parameter_input":
				param_type=tool_state['parameter_type']
				if 'default' in tool_state.keys():
					def_value=tool_state['default']
				else:
					def_value=''
			dic_parameters[name_param]=def_value


	with open(args.output_yaml, 'w') as f:
		for i in dic_parameters.keys():
			print('"'+i+'": '+str(dic_parameters[i]), file=f)

if __name__ == "__main__":
	main()
		