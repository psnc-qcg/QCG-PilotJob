import os
import logging

class JobModels:

    JOB_MODELS = {
        "threads": "preprocess_threads",
        "intelmpi": "preprocess_intelmpi",
        "openmpi": "preprocess_openmpi",
        "srunmpi": "preprocess_srunmpi",
        "default": "preprocess_default"
    }

    def get_model(self, model_name):
        preprocess_method = JobModels.JOB_MODELS.get(model_name)
        if not preprocess_method:
            raise ValueError(f'unknown job execution model "{model_name}"')
        return getattr(self, preprocess_method)

    def preprocess_common(self, appid, job_execution, allocation, env, workdir):
        if job_execution.stdin:
            job_execution.args.extend(["-i", os.path.join(workdir, job_execution.stdin)])
            job_execution.stdin = None

        if job_execution.stdout:
            if job_execution.args is not None:
                job_execution.args.extend(["-o", os.path.join(workdir, job_execution.stdout)])
            else:
                job_execution.args = ["-o", os.path.join(workdir, job_execution.stdout)]
            job_execution.stdout = None

        if job_execution.stderr:
            if job_execution.args is not None:
                job_execution.args.extend(["-e", os.path.join(workdir, job_execution.stderr)])
            else:
                job_execution.args = ["-e", os.path.join(workdir, job_execution.stderr)]
            job_execution.stderr = None

        if allocation.has_binding():
            env.update({'QCG_PM_CPU_SET': ','.join([str(c) for c in sum(
                [node['cores'] for node in allocation.nodes], [])])})

    def preprocess_threads(self, appid, job_execution, allocation, env, workdir):
        """Prepare execution description for threads execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = job_execution.exec
        job_args = job_execution.args

        env.update({'OMP_NUM_THREADS': str(allocation.cores)})

        core_ids = [str(core) for core in allocation.nodes[0]['cores']]
        #env.update({'GOMP_CPU_AFFINITY': ','.join(core_ids)})
        #env.update({'KMP_AFFINITY': 'explicit'})

        job_execution.exec = 'taskset'
        job_execution.args = ['-c', ','.join(core_ids), job_exec]
        if job_args:
            job_execution.args.extend(job_args)

        logging.debug(f'set GOMP_CPU_AFFINITY({",".join(core_ids)}), KMP_AFFINITY(explicit)')

    def preprocess_default(self, appid, job_execution, allocation, env, workdir):
        """Prepare execution description for default execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = job_execution.exec
        job_args = job_execution.args

        run_conf_file = os.path.join(workdir, ".{}.runconfig".format(appid))
        with open(run_conf_file, 'w') as conf_f:
            conf_f.write("0\t%s %s\n" % (
                job_exec,
                ' '.join('{0}'.format(str(arg).replace(" ", "\\ ")) for arg in job_args) if job_args else ''))
            if allocation.cores > 1:
                if allocation.cores > 2:
                    conf_f.write("1-%d /bin/true\n" % (allocation.cores - 1))
                else:
                    conf_f.write("1 /bin/true\n")

        if allocation.has_binding():
            core_ids = []
            for node in allocation.nodes:
                core_ids.extend([str(core) for core in node['cores']])
            cpu_bind = "--cpu-bind=verbose,map_cpu:{}".format(','.join(core_ids))
        else:
            cpu_bind = "--cpu-bind=verbose,cores"

        job_execution.args = [
            "-n", str(allocation.cores),
            "--overcommit",
            "--mem-per-cpu=0",
            cpu_bind,
            "--multi-prog"]

        self.preprocess_common(appid, job_execution, allocation, env, workdir)

        job_execution.exec = 'srun'
        job_execution.args.append(run_conf_file)

    def preprocess_openmpi(self, appid, job_execution, allocation, env, workdir):
        """Prepare execution description for openmpi execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = job_execution.exec
        job_args = job_execution.args

        # create rank file
        if allocation.has_binding():
            rank_file = os.path.join(workdir, ".{}.rankfile".format(appid))
            rank_id = 0
            with open(rank_file, 'w') as rank_f:
                for node in allocation.nodes:
                    for core in node['cores']:
                        rank_f.write(f'rank {rank_id}={node.node.name} slot={core}\n')
                        rank_id = rank_id + 1

            job_execution.args = [
                '--rankfile',
                str(rank_file),
            ]
        else:
            job_execution.args = [
                '-n',
                str(allocation.cores),
            ]

        self.preprocess_common(appid, job_execution, allocation, env, workdir)

        job_execution.exec = 'bash'
        job_execution.args = ['-c', 'source /etc/profile & module load openmpi; exec mpirun {} {}'.format(
            job_exec, '' if not job_args else ' '.join(job_args))]

    def preprocess_intelmpi(self, appid, job_execution, allocation, env, workdir):
        """Prepare execution description for intelmpi execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = job_execution.exec
        job_args = job_execution.args

        mpi_args = []
        first = True

        # create rank file
        if allocation.has_binding():

            for node in allocation.nodes:
                if not first:
                    mpi_args.append(':')

                mpi_args.extend([
                    '-host',
                    f'{node["name"]}',
                    '-n',
                    f'{len(node["cores"])}',
                    '-env',
                    f'I_MPI_PIN_PROCESSOR_LIST={",".join([str(core) for core in node["cores"]])}',
                    f'{job_exec}',
                    *job_args])

                first = False

            env.update({'I_MPI_PIN': '1'})
        else:
            mpi_args = ['-n', f'{str(allocation.cores)}', f'{job_exec}']

        if logging.root.level == logging.DEBUG:
            env.update({'I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS':
                            '-vvvvvv --overcommit --oversubscribe --cpu-bind=none --mem-per-cpu=0'})
            env.update({'I_MPI_HYDRA_DEBUG': '1'})
            env.update({'I_MPI_DEBUG': '5'})
        else:
            env.update({'I_MPI_HYDRA_BOOTSTRAP_EXEC_EXTRA_ARGS':
                            '-v --overcommit --oversubscribe --mem-per-cpu=0'})

        self.preprocess_common(appid, job_execution, allocation, env, workdir)

        job_execution.exec = 'mpirun'
        job_execution.args = [*mpi_args]
        if job_args:
            job_execution.args.extend(job_args)

    def preprocess_srunmpi(self, appid, job_execution, allocation, env, workdir):
        """Prepare execution description for mpi with slurm's srun execution model.

        Args:
            ex_job (ExecutionJob): job execution description
        """
        job_exec = job_execution.exec
        job_args = job_execution.args

        # create rank file
        if allocation.has_binding():
            cpu_masks = []
            for node in allocation.nodes:
                for slot in node["cores"]:
                    cpu_mask = 0
                    for cpu in slot.split(','):
                        cpu_mask = cpu_mask | 1 << int(cpu)
                    cpu_masks.append(hex(cpu_mask))
            cpu_bind = "--cpu-bind=verbose,mask_cpu:{}".format(','.join(cpu_masks))
        else:
            cpu_bind = "--cpu-bind=verbose,cores"

        job_execution.exec = 'srun'
        job_execution.args = [
            "-n", str(allocation.cores),
            "--overcommit",
            "--mem-per-cpu=0",
            "-m", "arbitrary",
            cpu_bind ]

        self.preprocess_common(appid, job_execution, allocation, env, workdir)

        job_execution.args.append(job_exec)
        if job_args:
            job_execution.args.extend(job_args)
