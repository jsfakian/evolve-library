#!/usr/bin/python

# Author: Michail Flouris michail.flouris@onapp.com
# License: All rights reserved - Confidential code
import sys, os, subprocess, shlex, time, shutil, inspect
import requests # for http requests  
import yaml		# yaml process / check
from collections import OrderedDict 
#import docker # need docker lib / API


# FIXME from the Evolve F2F in Munich!!!:
'''
Most important: Port ALL yaml files of partners as a FIRST priority
-> ask yaml from Thales & Memex (Neurocom) & Space!!
-> Port workloads with Sensors, e.g. from 

'''

argoheader='''apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  generateName: %s-
spec:
  imagePullSecrets:
  - name: %s
'''
sensorheader='''apiVersion: argoproj.io/v1alpha1
kind: Sensor
metadata:
  name: %s
  labels:
    # sensor controller with instanceId "argo-events" will process this sensor
    sensors.argoproj.io/sensor-controller-instanceid: argo-events
    # sensor controller will use this label to match with it's own version
    # do not remove
    argo-events-sensor-version: v0.10
spec:
  template:
    spec:
      containers:
        - name: "sensor"
          image: "argoproj/sensor"
          imagePullPolicy: Always
      serviceAccountName: argo-events-sa
'''

sensordep='''  eventProtocol:
    type: "HTTP"
    http:
      port: "9300"
  dependencies:
    - name: "%s"
'''

sensortriggers='''  triggers:
    - template:
        name: %s
'''

sensorpolicy_backoff='''        policy:
          # Backoff before checking the resource labels
          backoff:
            # Exit with error after this many steps
            steps: %s
'''
sensorflow='''        group: argoproj.io
        version: v1alpha1
        kind: Workflow
        source:
          inline: |
            apiVersion: argoproj.io/v1alpha1
            kind: Workflow
            metadata:
              namespace : wp6-p4
              generateName: %s
            spec:
              ttlSecondsAfterFinished: 100
              %s
              entrypoint: %s
              arguments:
                parameters:
'''

sfargumentname='''                - name: %s
'''
sfargumentvalue='''                  value: %s
'''

sfvolume='''              volumes:
                - name: %s
                  persistentVolumeClaim:
                    claimName: %s
'''

sftemplate='''              templates:
'''

sftemplatename='''              - name: %s
'''

sfinputs='''                inputs:
                  parameters:
'''
sfinputs_arg='''                    - name: %s
'''

sfoutputs='''                  outputs:
                    parameters:
'''
sfoutputs_arg='''                      - name: %s
                        valueFrom:
                          path: %s
'''
sfstephead='''                steps:
'''

sfstep='''                - - name: %s
                    template: %s
''' 

sfsteparg='''                    arguments:
                      parameters:
                        - name: %s
                          value: "%s"
'''

sfsteploop='''                        withItems:
'''

sfsteploopitem='''                          - %s
'''

sfscript='''                  script:
                    image: %s
                    imagePullPolicy: Always
                    command: ["%s"]
                    source: |
					  %s
'''
sfscriptvolume='''                    volumeMounts:
                      - name: %s
                        mountPath: "/data"
'''

sfresource='''                resource:
                  action: %s
'''

sfresourcecond='''                  successCondition: status.phase == Succeeded
                  failureCondition: status.phase in (Failed, Error)
'''

sfresourceman='''                  manifest: |
                    apiVersion:  v1
                    kind: Pod
                    metadata:
                      name: %s
                      namespace : wp6-p4
'''

sfcontainer='''                container:
                  ttlSecondsAfterFinished: 100
                  imagePullPolicy: Always
                  image: %s
                  command: ["%s"]
                  args: %s
'''

sensorres='''      resourceParameters:
        - src:
            event: "%s"
          dest: %s
'''

volhead='''  volumes:
%s
'''
volheadpath='''     - name: %s
        hostPath:
        path: %s
'''
volheadclaim='''     - name: %s
           persistentVolumeClaim:
           claimName: %s
'''
volheadnfs='''      - name: %s
        nfs:
            server: %s
            path: %s
'''
volheadmap='''      - name: %s
        %sMap:
          name: %s
          items:%s
'''

inputs='''    inputs:
      parameters:
'''
inputs_arg='''      - name: %s
'''

outputs='''    outputs:
      parameters:
'''
outputs_arg='''      - name: %s
        valueFrom:
          path: %s
'''

params='''        arguments:
          parameters:
'''
params_arg='''          - name: %s
            value: "%s"
'''

temphead='''  templates:
  - name: %s
    steps:
'''
tempstagehead='''    - - name: %s
        template: %s
'''
parallelstagehead='''      - name: %s
        template: %s
'''
loophead='''        arguments:
          parameters:
          - name: command
            value: "{{item.command}}"
          - name: arguments
            value: "{{item.argument}}"
        withItems:
'''

loopitem='''        - { command: '%s', argument: '%s' }
'''


stagehead='''    container:
      image: %s%s%s
      command: %s
      args: %s%s
'''

script='''    script:
      image: %s
      command: [%s]
      source: |
        %s
'''

class sensorflow_template (object):
	def __init__(self, inputs, outputs):
		self.input = inputs
		self.output = outputs
		self.steps = []
		self.scripts = []
		self.resources = []
		self.containers = []



class Usage(Exception):
	def __init__(self, msg):
		self.msg = msg

def get_config(cfgfile, cfgkeys):
	if not os.path.isfile(cfgfile):
		raise Exception("init ERROR: config file %s does not exist!" % cfgfile )

	config = {}
	try:
		with open(cfgfile, 'r') as stream:
			try:
				cfgdata = yaml.safe_load(stream)
				# print cfgdata # DEBUG
			except yaml.YAMLError as exc:
				print(exc)
			for k in cfgkeys:
				if k not in cfgdata.keys():
					raise Exception("config ERROR: could not find field %s in config-file: %s !" % (k,cfgfile) )
				config[k] = cfgdata[k]
		return config
	except Exception as e:
		raise Exception('Could not load config file: %s' % str(e))

def _shexec(cmd, expected_ret=0, verbose=False, _shell=False, dontsplit=False):
    """Executes a command and returns a tuple containing the return code,
    stdout, and stderr."""
    
    _cmd = cmd
    if dontsplit==False and not isinstance(cmd, list):
        #_cmd = shlex.split(str(cmd.encode('ascii')))
	    _cmd = shlex.split(cmd)
        
    if verbose:
        print("_shexec() - _cmd: '%s' _shell: %s" % (_cmd, _shell))
                      
    # shlex.split doesn't work in Unicode strings
    proc = subprocess.Popen(_cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, \
        stderr=subprocess.PIPE, close_fds=True, shell=_shell)
    
    stdout, stderr = proc.communicate()
    proc.wait()
    if verbose:
        print("_shexec() - \t%s" % str(stdout))
    if expected_ret != None and proc.returncode != expected_ret:
        if verbose:
            print("_shexec() - ERROR: \t%s" % stderr)
        raise Exception("_shexec() - failed to run %s: %s (%s)" % (cmd, stderr, stdout))
    return (proc.returncode, stdout, stderr)

def _parseout(output, stopnotfound=True):
	pstat = dict()
	#for line in output.splitlines(0):
	for line in output.decode().splitlines(0):
		if stopnotfound and ":" not in line:
			break
		#print "Splitting:",line
		name,val = line.split(":",1)
		pstat[name.strip().lower()] = val.strip().lower()
	return pstat

def _parseoutspace(output, firstlinefields=False, stopnotfound=True):
	pstat = dict()
	firstline = True
	fieldnames = []
	items = []
	#for line in output.splitlines(0):
	for line in output.decode().splitlines(0):
		if stopnotfound and " " not in line:
			break
		#print "Splitting:",line
		fdata = line.split()
		if firstlinefields:
			if firstline:
				firstline = False
				fieldnames = [ f.lower() for f in fdata ]
			else:
				zipObj = zip(fieldnames, fdata) # zip object from two lists
				jdata = dict(zipObj) # dict from zip object (list of tuples)
				items.append( jdata )
		else:
			items.append( fdata )
	return items, fieldnames

def sleep(seconds):
	try:
		if seconds < 1 or seconds > 10000:
			raise Exception("Invalid value %s for sleep seconds" % str(seconds));
		print ("Sleeping for %s seconds" % seconds)
		time.sleep(seconds)
	except Usage as err:
		print >> sys.stderr, err.msg
		return None
	return True

def wait(seconds):
	return sleep(seconds)

def error(message):
	try:
		raise Exception()
	except Exception as e:
		print("EVOLVE EXCEPTION: %s\n\n" % str(message))
		sys.exit(1)

def abort(message):
	try:
		raise Exception()
	except Exception as e:
		print("EVOLVE ABORT: %s\n\n" % str(message))
		sys.exit(2)

def get_default_config(cfgfile):
	# ATTENTION: must match the ourput tuple fields and order
	cfgkeys = [ 'registry', 'host', 'workflow_engine', 'paths' ]

	if not os.path.isfile(cfgfile):
		bsconfig = os.path.basename(cfgfile)
		cfound = False
		# check also /etc/cfgfile and # /usr/local/zeppelin/cfgfile
		cfgpaths = [ "/etc/%s" % bsconfig, "/usr/local/zeppelin/%s" % bsconfig ]
		for cf in cfgpaths:
			if os.path.isfile(cf):
				cfound = True
				cfgfile = cf
		if not cfound:
			raise Exception("init ERROR: config file %s does not exist!" % cfgfile )

	config = get_config( cfgfile, cfgkeys )
	#print "CONFIG", config # DEBUG
	if 'url' not in config['registry'].keys() or not config['registry']['url']:
		config['registry']['url'] = "%s:%s" % ( config['registry']['host'], config['registry']['port'] )
	registry = config['registry']
	workflow_engine = config['workflow_engine']
	paths = config['paths']
	host = config['host']

	return (config, registry, workflow_engine, paths, host)

# =================================================================
# the job object
class job(object):

	def __init__(self, workname, workflow):
		#Init function of the evolve job for a specific workflow

		if not workname or not workflow:
			raise Exception("init ERROR: invalid workflow or job name")

		self.workflow = workflow
		self.config = workflow.config
		self.registry = workflow.registry
		self.workflow_engine = workflow.workflow_engine
		self.paths = workflow.paths
		self.host = workflow.host

		#Init function of the evolve job
		self.workname = workname
		self.runstate = None
		print("Job %s init: OK" % self.workname)

	def __str__(self):
		return str(self.workname)

	def status(self, silent=False):
		try:
			if not silent:
				print ("Getting status of workflow job '%s'" % self.workname )
			(retcode, retstdout, retstderr) = _shexec("argo get %s" % self.workname)
			if not silent:
				print ("%s" % retstdout)
			self.runstate = _parseout(retstdout)
			#assert( self.workname == self.runstate['name'] )
			if not silent:
				print ("Workflow job status: %s" % self.runstate['status'])
		except Usage as err:
			print >> sys.stderr, err.msg
			return "error"
		return self.runstate['status']

	def inprogress(self, silent=True):
		return self.active(silent)

	def active(self, silent=True):
		try:
			if not silent:
				print ("Checking workflow job '%s'" % self.workname )
			(retcode, retstdout, retstderr) = _shexec("argo get %s" % self.workname)
			if not silent:
				print ("%s" % retstdout)
			self.runstate = _parseout(retstdout)
			#assert( self.workname == self.runstate['name'] )
			if not silent:
				print ("Workflow job status: %s" % self.runstate['status'])
			if self.runstate['status'] not in ['running']:
				return False
		except Usage as err:
			print >> sys.stderr, err.msg
			return False
		return True

	def wait_for_completion(self, checksec=30, silent=False, timeoutsec=3000):
		try:
			if not silent:
				print ("Waiting for completion of job '%s'" % self.workname )
			exitok=False
			checktime=0
			while checktime < timeoutsec:
				(retcode, retstdout, retstderr) = _shexec("argo get %s" % self.workname)
				if not silent:
					print ("%s" % retstdout.decode("utf-8"))
				self.runstate = _parseout(retstdout)
				if 'status' not in self.runstate.keys(): # check desirable fields
					raise Exception("Error parsing remote state")
				#assert( self.workname == self.runstate['name'] )
				if not silent:
					print ("[%s sec] Workflow job status: %s" % (str(checktime), self.runstate['status']) )
				if self.runstate['status'] not in ['running']:
					if self.runstate['status'] not in ['succeeded','success']:
						print ("ERROR: Workflow job FAILED with message %s" % ( self.runstate['message']) )
						break
					else: # succeeded
						print ("Workflow job execution SUCCESSFUL")
						exitok=True
						break
				time.sleep(checksec)
				checktime += checksec
		except Usage as err:
			print >> sys.stderr, err.msg
			return exitok
		return exitok

	def get_results(self, resultpath, silent=False):
		try:
			if not silent:
				print ("Receiving results of job '%s'" % self.workname )
			if self.active():
				print ("Job '%s' still in progress!" % self.workname )
			# FIXME : retrieve results from remote path name!! use scp?
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return None

	def terminate(self, silent=False):
		try:
			if not silent:
				print ("Terminating job '%s'" % self.workname )
			(retcode, retstdout, retstderr) = _shexec("argo terminate %s" % self.workname)
			if not silent:
				print ("%s" % retstdout)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	def kill(self, silent=False):
		return self.terminate(silent)

# =================================================================
# the workflow object
class workflow(object):

	def __init__(self, name, secrets='regcred', cfgfile='evolve.conf', sensor = False, sensorinit = {}):
		#Init function of the evolve workflow

		self.cfgfile = cfgfile
		(self.config, self.registry, self.workflow_engine, self.paths, self.host) = get_default_config(cfgfile)

		if self.registry and self.workflow_engine and self.paths and self.host:
			print("Evolve config: OK")
		else:
			raise Exception("init ERROR: config failed for registry or workflow engine! (config-file: %s)" % cfgfile )

		#Init function of the evolve workflow
		self.name = name
		self.secrets = secrets
		self.sensor = sensor
		self.workfile = self.paths['workfile'] # FIXME: check what we get from config!!
		if self.workflow_engine == 'argo' or self.workflow_engine == 'argoflow':
			if self.sensor:
				self.header = sensorheader
			else:	
				self.header = argoheader
		else:
			raise Exception("init ERROR: invalid workflow engine %s" % self.workflow_engine )
		

		if self.sensor : 
			self.sensordep = sensordep
			self.sensortriggers = sensortriggers
			self.sensorpolicy_backoff = sensorpolicy_backoff
			self.sensorflow = sensorflow
			self.sfargumentname = sfargumentname
			self.sfargumentvalue = sfargumentvalue
			self.sfvolume = sfvolume
			self.sftemplate = sftemplate
			self.sftemplatename = sftemplatename
			self.sfinputs = sfinputs
			self.sfinputs_arg = sfinputs_arg
			self.sfoutputs = sfoutputs
			self.sfoutputs_arg = sfoutputs_arg
			self.sfstephead = sfstephead
			self.sfstep = sfstep
			self.sfsteparg = sfsteparg
			self.sfsteploop = sfsteploop
			self.sfsteploopitem = sfsteploopitem
			self.sfscript = sfscript
			self.sfscriptvolume = sfscriptvolume
			self.sfresource = sfresource
			self.sfresourcecond = sfresourcecond
			self.sfresourceman = sfresourceman
			self.sfcontainer = sfcontainer
			self.sensorres = sensorres
			self.templates = OrderedDict()
			self.dependencies = sensorinit.get('dependencies')
			self.triggername = name + "-trigger"
			self.backoff_steps = sensorinit.get('backoff')
			self.sensor_generate_name = name + "-"

			if 'ImagePullSecrets' in sensorinit:
				self.sensor_ImagePullSecrets = """imagePullSecrets:
                - name: %s""" %(sensorinit.get('ImagePullSecrets'))
			else: 
				self.sensor_ImagePullSecrets = ""

			#self.sensor_entrypoint = sensorinit.get('entrypoint')
			self.sensor_params = sensorinit.get('sensor_params')
			self.sensor_volume_name =  sensorinit.get('sensor_volume_name')
			self.sensor_volume_claim_name =  sensorinit.get('sensor_volume_claim_name')
			self.res_event = sensorinit.get('res_event')
			self.res_dest =  sensorinit.get('res_dest')

		else:
			self.volhead = volhead
			self.volheadpath = volheadpath
			self.volheadnfs = volheadnfs
			self.volheadmap = volheadmap
			self.temphead = temphead
			self.tempstagehead = tempstagehead
			self.loophead = loophead
			self.loopitem = loopitem
			self.inputs = inputs
			self.inputs_arg = inputs_arg
			self.outputs = outputs
			self.outputs_arg = outputs_arg
			self.params = params
			self.params_arg = params_arg
			self.parallelstagehead = parallelstagehead
			self.stagehead = stagehead
			self.script = script
			self.volumes = []
			self.stages = []
			self.scripts = []
			self.runstate = None

		print("Workflow init: OK")

	def __str__(self):
		return str(self.name)


	def addsensortemplate (self, templatename, inputs = "", outputs = ""):
		"""Create new sensoflow template that will contain steps, scripts, containers or resources"""
		self.templates[templatename] = sensorflow_template(inputs = inputs, outputs = outputs)
		return True


	def addsensorstep(self, templatename, name, template, loopitems = [], args = []):
		"""Add a workflow step"""
		if templatename not in self.templates:
			raise Exception("ERROR: template not found")
		if len(self.templates[templatename].steps) == 0:
			self.sensor_entrypoint = templatename
		self.templates[templatename].steps.append({'name' : name, 'template' : template, 'args' : args, 'loopitems' : loopitems})
		return True
	
	def addsensorscript(self, templatename, name, image, command, source, inputs=[], outputs=[], volume = ""):
		"""Add a workflow script"""
		if templatename not in self.templates:
			raise Exception("ERROR: template not found")
		self.templates[templatename].scripts.append({'name' : name, 'inputs' : inputs, 'outputs' : outputs, 'image' : image, 'command': command, 'source': source, 'volume' : volume})
		return True

	def addsensorresource (self, templatename, name, action, manifest ,condition = False):
		"""Add a workflow resource"""
		if templatename not in self.templates:
			raise Exception("ERROR: template not found")
		self.templates[templatename].resources.append({'name': name, 'action': action, 'condition' : condition , 'manifest' : manifest})
		return True

	def addsensorcontainer (self, templatename, name, inputs, image, command, args):
		"""Add a workflow container"""
		if templatename not in self.templates:
			raise Exception("ERROR: template not found")
		self.templates[templatename].containers.append({'name': name, 'inputs': inputs, 'image': image, 'command': command, 'args': args})
		return True


	def addvolume(self, name, path='', server='localhost', vtype='local', mapname=None, mapitems=None):
		"""Add a workflow volume with attributes: type, server, path, etc."""
		self.volumes.append( {'name': name, 'vtype': vtype, 'server': server, 'path': path, 'mapname': mapname, 'mapitems': mapitems} )
		return True
 
	def addstage(self, name, image, command, args="", params={}, inputs = "", outputs = "", volumes='', 
					resources='', env='', parallel=False, conditional={}, script = ""):
		"""Add a workflow stage with image, volume, etc."""
		self.stages.append( {'name': name, 'image': image, 'env': env, 'resources': resources, 'command': command, 'inputs': inputs, 
							 'outputs': outputs, 'args': args, 'params':params, 'volumes': volumes, 'parallel':parallel, 
							 'conditional':conditional, "script":script})
		return True
		
	def save(self, silent=False):
		"""Save the workflow into a yaml file"""
		if not self.workfile:
			raise Exception("ERROR: invalid workfile path!")
		if self.sensor:
			raise Exception("For sensors use save_sensor")
		try:
			with open( self.workfile, 'w') as fw:
				fw.write(self.header % (self.name, self.secrets) )
				fw.write('  entrypoint: %s\n' % self.name)
				volstr = ''
				for vol in self.volumes:
					if vol['vtype'] == 'nfs':
						volstr = volstr + self.volheadnfs % ( vol['name'], vol['server'], vol['path'])
					elif vol['vtype'] == 'map':
						mapitvol = ''
						if vol['mapitems']:
							keyf = False
							for key in vol['mapitems'].keys():
								if key == 'key':
									mapitvol = mapitvol + '\n          - %s: %s' % ( key, vol['mapitems'][key] )
									keyf = True
							if not keyf:
								raise Exception("map requires a \'key\' field!")
							for key in vol['mapitems'].keys():
								if key == 'key':
									continue
								mapitvol = mapitvol + '\n            %s: %s' % ( key, vol['mapitems'][key] )
						volstr = volstr + self.volheadmap % ( vol['name'], vol['name'], vol['mapname'], mapitvol)
					elif vol['vtype'] == 'claim':
						volstr = volstr + self.volheadlaim % ( vol['name'], vol['name'])
					else:
						volstr = volstr + self.volheadpath % ( vol['name'], vol['path'])
				if len(volstr):
					fw.write('  volumes:\n%s' % volstr )

				fw.write(self.temphead % self.name)
				conditionals = {}
				for st in self.stages:
					if st['conditional']:
						if not st['conditional']['id'] in conditionals:
							conditionals[st['conditional']['id']] = []
						conditionals[st['conditional']['id']].append(st['conditional'])
					if st['parallel'] == True or (st['conditional'] and len(conditionals[st['conditional']['id']]) > 1):
						fw.write(self.parallelstagehead % (st['name'], st['name']))
					else :
						fw.write(self.tempstagehead % (st['name'], st['name']))
					if "name" in st['params']:
						fw.write(self.params)
						if len(st["params"]["name"]) != len(st["params"]["value"]):
							raise Exception("The paramater name and path of a step, should have the same length.")
						else:
							for i, _ in enumerate(st["params"]["name"]):
								fw.write(self.params_arg % (st["params"]["name"][i], st["params"]["value"][i]))
					if st['conditional']:
						fw.write("        when: %s\n" % st['conditional']['when'])
				for st in self.stages:
					if not type(st['command']) == list:
						command = st['command']
						if type(command) == tuple:
							command = str(list(command)).replace("'", '"')
						args = st['args']
					elif len(st['command']) != len(st['args']):
						raise Exception("ERROR: The commands and args do not match!")
					else:
						fw.write(self.loophead)
						for i, command in enumerate(st['command']):
							if type(command) == tuple:
								command = str(list(command))
							fw.write(self.loopitem % (command, str(st['args'][i])))
						command = "{{inputs.parameters.command}}"
						args = "{{inputs.parameters.argument}}"
					fw.write("\n")
					argstr = str(args)
					envstr = ''
					if st['env']:
						envstr = '\n      env:'
						for env in st['env']:
							for key in env.keys():
								if isinstance( env[key], (int, long)):
									envstr = envstr + '\n      - name: %s\n        value: %d' % ( key, env[key] )
								else:
									envstr = envstr + '\n      - name: %s\n        value: "%s"' % ( key, env[key] )
					resourcestr = ''
					if st['resources']:
						resourcestr = '\n      resources:\n          limits:'
						for resi in st['resources']:
							for key in resi.keys():
								if isinstance( resi[key], (int, long)):
									resourcestr = resourcestr + '\n              %s: %d' % ( key, resi[key] )
								else:
									resourcestr = resourcestr + '\n              %s: "%s"' % ( key, resi[key] )
					stagevolstr = ''
					if st['volumes']:
						stagevolstr = '\n      volumeMounts:'
						for vol in st['volumes']:
							stagevolstr = stagevolstr + '\n      - name: %s\n        mountPath: "%s"' % ( vol['name'], vol['mountPath'] )

					fw.write("  - name: %s\n" % st['name'])

					if len(st["inputs"]) != 0:
						fw.write(self.inputs)
						for name in st["inputs"]:
							fw.write(self.inputs_arg % (name))

					if len(st["outputs"]) !=0:
						fw.write(self.outputs)
						if len(st["outputs"]["name"]) != len(st["outputs"]["path"]):
							raise Exception("The output name and path of a template, should have the same length.")
						else:
							for i, _ in enumerate(st["outputs"]["name"]):
								fw.write(self.outputs_arg % (st["outputs"]["name"][i], st["outputs"]["path"][i]))
					if st['script']=="":
						fw.write(self.stagehead % (st['image'], resourcestr, envstr, command, argstr, stagevolstr) )
					else:
						if os.path.isfile(st['script']):
							script = open(st['script'], 'r').read()
						else:
							script = st['script']
						script = script.replace("\n", "\n        ")
						fw.write(self.script % (st['image'], command, script) )
				#fw.write("\n")
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		print("Workflow save: OK")
		return True



	def save_sensor(self, silent=False):
		"""Save the workflow into a yaml file"""
		func = inspect.currentframe().f_back.f_code
		if not self.workfile:
			raise Exception("ERROR: invalid workfile path!")
		if not self.sensor:
			raise Exception("For normal workflows use save")
		try:
			with open( self.workfile, 'wb') as fw:
				fw.write(self.header % (self.name) ) # witre the header up to the name
				fw.write(self.sensordep % (self.dependencies)) # write the dependencies 
				fw.write(self.sensortriggers % (self.triggername)) # triggers
				if self.backoff_steps: # some sensors might utilize policy such us backoff
					fw.write(self.sensorpolicy_backoff % (self.backoff_steps) )
				 # From this point on the sensor utilizes its workflow similar to normal workflows but not really
				fw.write(self.sensorflow % (self.sensor_generate_name, self.sensor_ImagePullSecrets, self.sensor_entrypoint))
				# name is a standard
				for name in self.sensor_params:
					fw.write(self.sfargumentname % (name['name']) )
					if 'value' in name:
						fw.write(self.sfargumentvalue % (name['value']) )
				#sensor workflows (call them sensorflows (sf)) can claim persistent volume for storage
				if self.sensor_volume_name :
					fw.write(self.sfvolume % (self.sensor_volume_name, self.sensor_volume_claim_name) )
				# the beggining of the sensorflow is pretty much standard
				fw.write(self.sftemplate)
				# The way to go about this is to devide all the sf in templates, each will have inputs outputs and
				# steps, or a container, or a script, or a resource. Each of those will have attributes like name, volume,
				# or other data like and executable script 
				for sf in self.templates:
					sfname = self.templates[sf]
					fw.write(self.sftemplatename % (sf) )
					if len(sfname.input) != 0:
						fw.write(self.sfinputs)
						for name in sfname.input:
							fw.write(self.sfinputs_arg % (name) )
					if len(sfname.output) !=0:
						fw.write(self.sfoutputs)
						if len(sfname.output["name"]) != len(sfname.output["path"]):
							raise Exception("The output name and path of a template, should have the same length.")
						else:
							for i, _ in enumerate(sfname.output["name"]):
								fw.write(self.sfoutputs_arg % (sfname.output["name"][i], sfname.output["path"][i]) )
					if sfname.steps:
						fw.write(self.sfstephead)
					for step in sfname.steps:
						fw.write(self.sfstep % (step["name"], step["template"]))
						for steparg in step["args"]:
							fw.write(self.sfsteparg % (steparg['name'], steparg['value']) )
						if step["loopitems"]:
							fw.write(self.sfsteploop)
							for item in step["loopitems"]:
								fw.write(self.sfsteploopitem % (item) )
					for script in sfname.scripts:
						fw.write("              - name: %s\n" % script["name"])
						if len(script["inputs"]) != 0:
							fw.write(self.sfinputs)
							for name in script["inputs"]:
								fw.write(self.sfinputs_arg % (name) )
						if len(script["outputs"]) !=0:
							fw.write(self.sfoutputs)
							if len(script["outputs"]["name"]) != len(script["outputs"]["path"]):
								raise Exception("The output name and path of a template, should have the same length.")
							else:
								for i, _ in enumerate(script["outputs"]["name"]):
									fw.write(self.sfoutputs_arg % (script["outputs"]["name"][i], script["outputs"]["path"][i]) )
						fw.write(self.sfscript % (script["image"], script["command"], script["source"]) )
						if script["volume"]: 
							fw.write(self.sfscriptvolume % script["volume"])
					for resource in sfname.resources: 
						fw.write("              - name: %s\n" % resource["name"])
						fw.write(self.sfresource % (resource["action"]))
						if resource["condition"]:
							fw.write(self.sfresourcecond)
						fw.write(self.sfresourceman % (resource["manifest"]) )
					for container in sfname.containers:
						fw.write("              - name: %s\n" % container["name"])
						if len(container["inputs"]) != 0:
							fw.write(self.sfinputs)
							for name in container["inputs"]:
								fw.write(self.sfinputs_arg % (name))
						fw.write(self.sfcontainer % (container["image"], container["command"], container["args"]))

				fw.write(self.sensorres % (self.res_event, self.res_dest) ) # witre the footer (resource parameters)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		print("Workflow save: OK")
		return True



	def load(self, pathname, silent=False):
		"""Load the workflow from an existing yaml file"""
		if not self.workfile:
			raise Exception("ERROR: invalid workfile path!")
		try:
			if not os.path.isfile(pathname):
				raise Exception("ERROR: failed to load invalid or non-existent yaml file path!")
			shutil.copyfile(pathname, self.workfile)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		print("Workflow load: OK")
		return True

	def show(self, silent=False):
		"""Print the configured workflow"""
		strout = "name=%s\nvolumes=%s\nstages=%s\n" % (self.name, str(self.volumes),  str(self.stages))
		if not silent:
			print (strout)
		return strout

	def run(self, wait=False, silent=False):
		""" Run the workflow from the specified yaml file, return job object"""
		if not self.workfile or not os.path.isfile(self.workfile):
			raise Exception("ERROR: invalid or non-existent workfile! is workflow saved?")
		# FIXME: check the yaml file?
		workname = None
		newjob = None
		try:
			print ("Starting execution of workflow '%s'" % self.name)
			if not wait:
				(retcode, retstdout, retstderr) = _shexec("argo submit %s" % self.workfile)
			else:
				(retcode, retstdout, retstderr) = _shexec("argo submit --wait %s" % self.workfile)
			print ("%s" % (retstdout).decode("utf-8"))
			self.runstate = _parseout(retstdout)
			workname = self.runstate['name']
			if workname:
				newjob = job( workname, self )
		except Usage as err:
			print >> sys.stderr, err.msg
		return newjob


# Docker lib calls:
# >>> client.containers.list()
# [<Container '45e6d2de7c54'>, <Container 'db18e4f20eaa'>, ...]
# >>> container = client.containers.get('45e6d2de7c54')
# >>> container.attrs['Config']['Image']
# "bfirsh/reticulate-splines"
# >>> container.logs()
# "Reticulating spline 1...\n"
# >>> container.stop()

# >>> client.images.pull('nginx')
# <Image 'nginx'>
# >>> client.images.list()
# [<Image 'ubuntu'>, <Image 'nginx'>, ...]

# remote docker image check:
# curl -u username:password -X GET https://evolve-registry.org:5050/v2/_catalog | jq -r
# to check the tags for an image:
# curl -u username:password -X GET https://evolve-registry.org:5050/v2/image/tags/list | jq -r


# =================================================================
# the Evolve cluster / testbed object
class cluster(object):

	def __init__(self, cfgfile='evolve.conf'):
		#Init function of the evolve cluster

		(self.config, self.registry, self.workflow_engine, self.paths, self.host) = get_default_config(cfgfile)

		if self.registry and self.workflow_engine and self.paths and self.host:
			print("Evolve config: OK")
		else:
			raise Exception("init ERROR: config failed for registry or workflow engine! (config-file: %s)" % cfgfile )

		# NOTE: assuming that we have done "docker login" from that user
		#self.docklient = docker.from_env()

		#print("Evolve connector init: OK")


	def __str__(self):
		return str(self.name)

	def get_info(self, silent=False):
		try:
			print ("\n==>>> Get info from %s" % self.registry['url'])
			(retcode, retstdout, retstderr) = _shexec("docker version")
			print ("Docker version:\n%s" % retstdout)
			# On Nova cluster:
			# vagrant@ubuntu-xenial:~/argoflow$ kubectl cluster-info
			# Kubernetes master is running at https://92.43.249.202:6443
			# CoreDNS is running at https://92.43.249.202:6443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy
			(retcode, retstdout, retstderr) = _shexec("kubectl cluster-info")
			print ("Kubernetes info:\n%s" % retstdout)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	def job_list(self, silent=True):
		try:
			if not silent:
				print ("\n==>>> List workflow jobs at %s" % self.registry['url'])
			(retcode, retstdout, retstderr) = _shexec("argo list")
			if retcode == 0:
				if not silent:
					print ("\n%s" % retstdout)
				jobs, jfields = _parseoutspace(retstdout, firstlinefields=True)
				if not silent:
					print ("Jobs:%s" % jobs)
			else:
				raise Exception("ERROR:\n%s" % retstderr)
			return jobs
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

	def job_status(self, jobname, silent=False):
		try:
			if not jobname:
				raise Exception("ERROR: invalid job name")
			print ("\n==>>> Check job at %s" % self.registry['url'])
			(retcode, retstdout, retstderr) = _shexec("argo get %s" % jobname)
			if not silent:
				print ("%s" % retstdout)
			runstate = _parseout(retstdout)
			if not silent:
				print ("Workflow job status: %s" % runstate['status'])
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return runstate['status']

	def job_exists(self, jobname, silent=False):
		try:
			if not jobname:
				raise Exception("ERROR: invalid job name")
			print ("\n==>>> Check job at %s" % self.registry['url'])
			(retcode, retstdout, retstderr) = _shexec("argo get %s" % jobname)
			if not silent:
				print ("%s" % retstdout)
			if retcode != 0:
				return False
			runstate = _parseout(retstdout)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	def job_kill(self, jobname, silent=False):
		try:
			if not jobname:
				raise Exception("ERROR: invalid job name")
			if not self.job_exists(jobname):
				return False
			(retcode, retstdout, retstderr) = _shexec("argo terminate %s" % jobname)
			if not silent:
				print ("%s" % retstdout)
			if retcode != 0:
				return False
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	def job_delete(self, jobname, silent=False):
		try:
			if not jobname:
				raise Exception("ERROR: invalid job name")
			if not self.job_exists(jobname):
				return False
			(retcode, retstdout, retstderr) = _shexec("argo delete %s" % jobname)
			if not silent:
				print ("%s" % retstdout)
			if retcode != 0:
				return False
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	#===============================================================================
	def image_list(self, silent=True):
		try:
			print ("\n==>>> List images from Registry %s" % self.registry['url'])
			#print str(self.docklient.images.list())
			#_shexec("scp %s %s:%s" % (self.workfile, self.host, self.port))

			#(retcode, retstdout, retstderr) = _shexec("docker image ls") # this is for LOCAL docker
			#print ("Image list:\n%s" % retstdout)

			#curlcmd = "curl -u %s:%s -X GET https://%s/v2/_catalog" % ( self.registry['user'], self.registry['pass'], self.registry['url'] )
			#(retcode, retstdout, retstderr) = _shexec(curlcmd) # REMOTE docker reg
			#if retcode == 0:
			#	print ("\n%s" % retstdout)

			# use requests for auth http call, then have the raw res.text, or res.json() output, the res.headers
			regurl = "https://%s/v2/_catalog" % self.registry['url']
			# NOTE: use verify=False to avoid the error caused by untrusted SSL certificates
			res = requests.get(regurl, auth=(self.registry['user'], self.registry['pass']), verify=False)
			#print ("Registry Images:\n%s\n" % str(res.json()) )
			retjson = res.json()
			repoimages = retjson['repositories']
			if not silent:
				for (no,img) in enumerate(repoimages):
					print ("%d. Image: %s" % (no, img) )
				print ("%d images found." % len(repoimages) )
			return repoimages
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

	def image_upload(self, imagename, tagname, silent=False):
		"""Upload new image with a tag on a specific image (repository) name"""
		try:
			if not imagename:
				raise Exception("ERROR: invalid image name")
			print ("\n==>>> Check image at %s" % self.registry['url'])
			# FIXME
			# Check out API at https://docs.docker.com/registry/spec/api/

			# You need to tag your image correctly first with your registryhost:
			# docker tag [OPTIONS] IMAGE[:TAG] [REGISTRYHOST/][USERNAME/]NAME[:TAG]
			# Then docker push using that same tag.
			# docker push NAME[:TAG]

			# Example:
			# docker tag my-image username/my-repo
			# docker push username/my-repo

			if not silent:
				print ("%s" % retstdout)
		except Usage as err:
			print >> sys.stderr, err.msg
			return None
		return True

	def image_info(self, imagename, silent=True):
		"""Listing info & tags for a specific image (repository)"""
		try:
			if not imagename:
				raise Exception("ERROR: invalid image name")
			if not silent:
				print ("\n==>>> Check image at %s" % self.registry['url'])
			# FIXME
			# Check out API at https://docs.docker.com/registry/spec/api/
			# GET /v2/<name>/tags/list
			regurl = "https://%s/v2/%s/tags/list" % (self.registry['url'], imagename)
			# NOTE: use verify=False to avoid the error caused by untrusted SSL certificates
			res = requests.get(regurl, auth=(self.registry['user'], self.registry['pass']), verify=False)
			res.raise_for_status() # raises exception on error
			retjson = res.json()
			#repoimages = retjson['repositories']
			if not silent:
				print ("Image Info: %s\n" % str(res.json()) )
			if 'errors' in retjson.keys():
				print ("ERROR:%s\n" % str(retjson['errors']) )
				return False
			return retjson
		except requests.exceptions.HTTPError as errh:
			print ("Http Error:",errh)
			return None
		except requests.exceptions.ConnectionError as errc:
			print ("Error Connecting:",errc)
			return None
		except requests.exceptions.Timeout as errt:
			print ("Timeout Error:",errt)
			return None
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

	def image_tags(self, imagename, silent=True):
		"""Listing tags for a specific image (repository)"""
		try:
			imginfo = self.image_info(imagename, silent)
			if imginfo:
				return imginfo['tags']
			else:
				return False
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

	def image_exists(self, imagename, silent=True):
		try:
			if not imagename:
				raise Exception("ERROR: invalid image name")
			if not silent:
				print ("\n==>>> Check image at %s" % self.registry['url'])
			imginfo = self.image_info(imagename, True)
			if imginfo:
				if not silent:
					print ("Image %s exists" % imagename )
				return True
			else:
				if not silent:
					print ("Image %s not found" % imagename )
				return False
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

	def image_delete(self, imagename, silent=False):
		try:
			if not imagename:
				raise Exception("ERROR: invalid image name")
			if not silent:
				print ("\n==>>> Check image at %s" % self.registry['url'])
			# FIXME
			# Check out API at https://docs.docker.com/registry/spec/api/
			# An image may be deleted from the registry via its name and reference. A delete may be issued with the following request format:
			# 
			# DELETE /v2/<name>/manifests/<reference>
			# 
			# For deletes, reference must be a digest or the delete will fail.
			# If the image exists and has been successfully deleted, the following response will be issued:
			# 
			# 202 Accepted
			# Content-Length: None
			# 
			return False # FIXME
			if not silent:
				print ("Image %s deleted" % imagename )
		except Usage as err:
			print >> sys.stderr, err.msg
			return None

