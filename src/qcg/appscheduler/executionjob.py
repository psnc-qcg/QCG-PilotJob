import asyncio
import logging
import uuid
import os
import json

from datetime import datetime
from string import Template

from qcg.appscheduler.joblist import JobExecution
from qcg.appscheduler.launcher.launcher import Launcher
import qcg.appscheduler.profile



class ExecutionJob:

    @profile
    def __init__(self, executor, envs, allocation, job):
        assert allocation is not None
        assert job is not None

        self.allocation = allocation
        self.job = job
        self.__envs = envs
        self.id = str(uuid.uuid4().hex)
        self.exitCode = None
        self.__executor = executor
        self.errorMessage = None
        self.envOpts = { }

        # temporary
        self.wdPath = '.'

        # inherit environment variables from parent process
        self.env = os.environ.copy()
#        self.env = {}

        self.nnodes = len(self.allocation.nodeAllocations)
        self.ncores = sum([node.ncores for node in self.allocation.nodeAllocations])
        self.nlist = ','.join([node.node.name for node in self.allocation.nodeAllocations])
        self.tasks_per_node = ','.join([str(node.ncores) for node in self.allocation.nodeAllocations])

        #logging.info("job resource's initialized {}, {}, {}, {}".format(self.nnodes, self.ncores, self.nlist, self.tasks_per_node))

        self.__setupJobVariables()

        # job execution description with variables replaced
        self.jobExecution = JobExecution(
            **json.loads(
                self.__substituteJobVariables(self.job.execution.toJSON())
            ))

    def __setupJobVariables(self):
        """
        Set variables that can be used in job description.
        """
        self.__jobVars = {
            'root_wd': self.__executor.base_wd,
            'ncores': str(self.ncores),
            'nnodes': str(self.nnodes),
            'nlist': self.nlist
        }

    def __substituteJobVariables(self, data):
        """
        Replace any variables used in job description.
        """
        if isinstance(data, str):
            return Template(data).safe_substitute(self.__jobVars)
        else:
            return json.loads(Template(json.dumps(data)).safe_substitute(self.__jobVars))


    def __setupSandbox(self):
        """
        Set a job's working directory based on execution description and root working directory of executor.
        An attribute 'wdPath' is set as an output of this method and directory is created
        """
        if self.jobExecution.wd is None:
            self.wdPath = self.__executor.base_wd
        else:
            self.wdPath = self.jobExecution.wd

            if not os.path.isabs(self.wdPath):
                self.wdPath = os.path.join(self.__executor.base_wd, self.wdPath)

#        logging.info("preparing job %s sanbox at %s" % (self.job.name, self.wdPath))
        if not os.path.exists(self.wdPath):
#            logging.info("creating directory for job %s at %s" % (self.job.name, self.wdPath))
            os.makedirs(self.wdPath)


    def __prepareEnv(self):
        """
        Setup an execution environment.
        Mostly environment variables are set
        """
        if self.jobExecution.env is not None:
            self.env.update(self.jobExecution.env)

        if hasattr(self.__executor, 'zmq_address'):
            self.env.update({
                'QCG_PM_ZMQ_ADDRESS': self.__executor.zmq_address
            })

#        logging.info('updating job\'s environment from {} objects'.format(str(self.__envs)))
        if self.__envs:
            for env in self.__envs:
                env.updateEnv(self, self.env, self.envOpts)

#        logging.info('environment after update: {}'.format(str(self.env)))


    def preprocess(self):
        """
        Prepare environment for job execution.
        Setup sandbox and environment variables.
        """
        self.__setupSandbox()
        self.__prepareEnv()


    def preStart(self):
        """
        This method should be executed before job will be started.
        The job statistics are updated (workdir, job start time) and executor is notified about job start.
        """
        self.__jobStartTime = datetime.now()

        self.__executor.taskExecuting(self)
        self.job.appendRuntime({'wd': self.wdPath})


    @profile
    async def launch(self):
        raise NotImplementedError('This method must be implemened in subclass')


    def postprocess(self, exitCode, errorMessage=None):
        """
        Postprocess job execution.
        Update job statistics (runtime), set the exit code and optionally error message and notify executor about job finish.
        """
        self.__jobStopTime = datetime.now()
        self.__jobRunTime = self.__jobStopTime - self.__jobStartTime
        self.job.appendRuntime({'rtime': str(self.__jobRunTime)})

        self.exitCode = exitCode
        self.errorMessage = errorMessage
        self.__executor.taskFinished(self)


    async def run(self):
        """
        Prepare environment for a job and launch job process.
        """
        try:
            self.preprocess()

            self.preStart()

            await self.launch()
        except Exception as ex:
            logging.exception("failed to start job %s" % (self.job.name))
            self.postprocess(-1, str(ex))


class LocalSchemaExecutionJob(ExecutionJob):

    @profile
    def __init__(self, executor, envs, allocation, job, schema):
        """
        Run application as a local process according to defined schema.
        """
        super().__init__(executor, envs, allocation, job)
        
        self.__schema = schema
        self.__processTask = None
        self.__stdinF = None
        self.__stdoutF = None
        self.__stderrF = None

        if schema:
            self.envOpts = schema.getEnvOpts()


    def preprocess(self):
        """
        Prepare environment for job execution.
        Setup environment and modify exec according to the execution schema.
        """
        super().preprocess()
        self.__schema.preprocess(self)


    async def __executeLocalProcess(self):
        try:
            je = self.jobExecution
            self.__stdinF = None
            self.__stdoutF = asyncio.subprocess.DEVNULL
            self.__stderrF = asyncio.subprocess.DEVNULL

            if je.stdin is not None:
                self.__stdinF = open(je.stdin, 'r')

            if je.stdout is not None:
                self.__stdoutF = open(os.path.join(self.wdPath, je.stdout), 'w')

            if je.stderr is not None:
                self.__stderrF = open(os.path.join(self.wdPath, je.stderr), 'w')

            logging.debug("launching process for job {}: {} {}".format(self.job.name, je.exec, str(je.args)))

            process = await asyncio.create_subprocess_exec(
                je.exec, *je.args,
                stdin=self.__stdinF,
                stdout=self.__stdoutF,
                stderr=self.__stderrF,
                cwd=self.wdPath,
    #            env={ **os.environ, **self.env }
                env=self.env
            )

#            logging.info("job %s launched" % (self.job.name))
            logging.info("local process for job {} launched".format(self.job.name))

            await process.wait()

            logging.info("local process for job {} finished".format(self.job.name))

            exitCode = process.returncode

            self.postprocess(exitCode)
        except Exception as e:
            self.postprocess(-1, str(e))


    @profile
    async def launch(self):
        self.__exec_task = asyncio.ensure_future(self.__executeLocalProcess())


    def postprocess(self, exitCode, errorMessage=None):
        for f in [self.__stdinF, self.__stdoutF, self.__stderrF]:
            if f is not None and f != asyncio.subprocess.DEVNULL:
                f.close()
        self.__stdinF = self.__stdoutF = self.__stderrF = None
        self.__exec_task = None

        super().postprocess(exitCode, errorMessage)


class LauncherExecutionJob(ExecutionJob):

    launcher = None

    @classmethod
    def StartAgents(cls, wdir, nodes):
        if cls.launcher:
            raise Exception('launcher agents already ininitialized')

        agents = [ { 'agent_id': node.name, 'slurm': { 'node': node.name } } for node in nodes ]
        cls.launcher = Launcher(wdir)

        asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(cls.launcher.start(agents)))


    @classmethod
    def StopAgents(cls):
        if cls.launcher:
            asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(cls.launcher.stop()))
            

    @profile
    def __init__(self, executor, envs, allocation, job):
        """
        Run application through the node launcher service.
        """
        super().__init__(executor, envs, allocation, job)
        self.envOpts = { 'nohostfile': True }


    @profile
    async def launch(self):
        """
        Submit job to the specific launcher agent.
        """
        je = self.jobExecution
        nodeAlloc = self.allocation.nodeAllocations[0]

        await self.__class__.launcher.submit(
                nodeAlloc.node.name,
                self.id,
                [ je.exec, *je.args ],
                stdin=os.path.join(self.wdPath, je.stdin) if je.stdin else None,
                stdout=os.path.join(self.wdPath, je.stdout) if je.stdout else None,
                stderr=os.path.join(self.wdPath, je.stderr) if je.stderr else None,
                env=self.env,
                wdir=self.wdPath,
                cores=nodeAlloc.cores,
                finish_cb=self.__finishCb)


    def __finishCb(self, message):
        self.postprocess(int(message.get('ec', -1)), message.get('message', None))

