from qcg.pilotjob.common.config import Var
from qcg.pilotjob.executor.parseres import get_resources
from qcg.pilotjob.common.reqsender import Request, RequestSender
from qcg.pilotjob.executor.queue import ScheduleQueue
from qcg.pilotjob.executor.jobs import Jobs
from qcg.pilotjob.executor.job import JobIterations
from qcg.pilotjob.executor.executor import Executor
from qcg.pilotjob.executor.iqevents import EventsListener
from qcg.pilotjob.inputqueue.job import JobState
from qcg.pilotjob.inputqueue.publisher import EventTopic
from qcg.pilotjob.executor.scheduler import Scheduler
from qcg.pilotjob.inputqueue.jobdesc import JobDescription
from qcg.pilotjob.executor.events import ExEventType, ExEvent
import qcg.pilotjob.executor.events as events

import logging
import asyncio
from enum import Enum


"""
1. gather information about local resources (sync)
2. register in global queue with local resources (sync)
3. periodically send heart beat (async)
4. get new jobs from global queue and place them in local queue (async)
5. after job finish (async):
   a) send back information to global queue
   b) send job stats to runtime monitoring
   c) remove job info from local queue
6. in the mean time execute jobs from local queue (ignore at all job dependencies) (sync)

Questions:
    - when to ask for new jobs ?
       - when all jobs are scheduled ?
       - when there are unused resources ?
       - ??

Executor parameters:
    name (str): optional - can be generated
    queue_address (str): address of the global queue
        wait (int): optional some time out for waiting on queue availability in cases where
            executors are started before global queue
    key (str): key used to authorize communication with global queue

    options:
        auto_finish (bool): in case where there are no ready jobs in global queue executor
            should finish automatically
"""

class IQInstance:

    def __init__(self, address, key, executor_name, manager):
        """The class used to communicate with InputQueue.

        Args:
            address (str): address of input queue
            key (str): key used to authorize
            executor_name (str): executor name under which to register in input queue
            manager (EXManager): manager instance
        """
        self.address = address
        self.key = key
        self.executor_name = executor_name
        self.manager = manager

        self.heart_beat_task = None
        self.heart_beat_interval = 30  # just for tests

        self.request4_new_iterations_lock = asyncio.Lock()

        self.iq_sender = RequestSender(self.address, event_publisher=events.event_publisher,
                                       send_event_type=ExEventType.IQ_REQUEST_SEND,
                                       recv_event_type=ExEventType.IQ_RESPONSE_RECV)

        self.iq_events_listener = None


    async def register(self, resources):
        """Register executor instance in InputQueue.

        Args:
            resources (Resources): the local resources available for executor
        """
        self.iq_sender.start()
        register_req = Request({'cmd': 'register_executor', 'executor': {'name': self.executor_name,
                                                                         'address': self.address,
                                                                         'checksum': 0,
                                                                         'resources': resources}}, with_event=True)
        self.iq_sender.send(register_req)

        result = await register_req.wait_for_result()
        logging.debug(f'got register result: {result}')

        ExEvent.register_iq({'result': result}).publish()

        if result and result.get('code') == 0:
            print(f'successfully connected to inputqueue @ {self.address}')
            self.start_heart_beat_notifier()

            if result.get('data', {}).get('events'):
                iq_events_address = result.get('data').get('events')
                logging.debug(f'got iq events address: {iq_events_address}')
                self.setup_events_listener(iq_events_address)
        else:
            self.iq_sender.stop()
            raise IOError('Failed to register')

    def setup_events_listener(self, iq_events_address):
        try:
            self.iq_events_listener = EventsListener(iq_events_address)
            self.iq_events_listener.register_topic(EventTopic.NEW_JOBS, self.new_jobs_in_input_queue)
            self.iq_events_listener.start()
        except:
            logging.error('failed to setup events listener')
            self.iq_events_listener = None

    def new_jobs_in_input_queue(self, new_jobs_event_data):
        logging.info('got notification from input queue about new jobs')
        ExEvent.iq_new_jobs({'event': new_jobs_event_data}).publish()
        self.manager.signal_schedule_event(Event(EventType.NEW_JOBS_IN_IQ))

    async def close(self):
        logging.info('closing iq communication instance')

        if self.heart_beat_task:
            await self.stop_heart_beat_notifier()

        if self.iq_sender:
            await self.iq_sender.stop()

        if self.iq_events_listener:
            await self.iq_events_listener.stop()

    def start_heart_beat_notifier(self):
        """Send heart beat signal to input queue.

        This method is automatically executed by task in the background.
        """
        if self.heart_beat_task:
            logging.warning('heart beat already started')
            return

        self.heart_beat_task = asyncio.ensure_future(self._heart_beat_notifier())

    async def stop_heart_beat_notifier(self):
        if not self.heart_beat_task:
            logging.warning('heart beat not started')
            return

        self.heart_beat_task.cancel()
        try:
            await asyncio.wait_for(self.heart_beat_task, 5)
        except asyncio.TimeoutError:
            logging.error('failed to stop heart beat notifier')
        except asyncio.CancelledError:
            pass
        finally:
            self.heart_beat_task = None

    async def _heart_beat_notifier(self):
        logging.info('heart beat notifier started')
        try:
            while True:
                logging.debug('sending heart beat notify to the input queue')
                ExEvent.heartbeat_signal(None).publish()
                self.iq_sender.send(Request({'cmd': 'executor_heartbeat',
                                             'executor': {'name': self.executor_name,
                                                          'checksum': 0}}))
                await asyncio.sleep(self.heart_beat_interval)
        except asyncio.CancelledError:
            raise
        finally:
            logging.info('finishing heart beat notifier')

    def request_new_iterations(self):
        ExEvent.request_new_its(None).publish()
        asyncio.ensure_future(self._send_request4_new_iterations())

    async def _send_request4_new_iterations(self):
        """Ask the input queue for new jobs."""
        try:
            async with self.request4_new_iterations_lock:
                new_iterations_req = Request({'cmd': 'get_iterations',
                                              'executor': {'name': self.executor_name,
                                                           'checksum': 0}}, with_event=True)
                self.iq_sender.send(new_iterations_req)

                result = await new_iterations_req.wait_for_result()
                if result.get('code') != 0:
                    raise IOError('Failed to obtain new jobs')

                jobs = result.get('data')
                self.manager.signal_schedule_event(Event(EventType.ENQUEUED_JOBS, jobs))
        except Exception as exc:
            logging.exception(f'failed to get new iterations: {str(exc)}')

    def notify_finished_jobs(self, jobs):
        """Notify input queue about finished jobs/iterations.

        Args:
            jobs (list(dict)): finished job iterations data, each entry should contain following keys:
                id (str): job identifier
                iteration (str,int): iteration index
                state (str): job state
        """
        self.iq_sender.send(Request({
            'cmd': 'iterations_finished',
            'executor': {'name': self.executor_name,
                         'checksum': 0},
            'jobs': jobs}))


class ResourceUsage:

    def __init__(self, resources, schedule_queue):
        """Track resource usage.

        Args:
            resources (Resources): available resources
            schedule_queue (ScheduleQueue): schedule queue
        """
        self.resources = resources
        self.schedule_queue = schedule_queue

    def get_total_cores(self):
        return self.resources.total_cores

    def get_used_cores(self):
        return self.resources.used_cores

    def get_pending_cores(self):
        """Return total number of cores of iterations pending in scheduling queue.

        Returns:
            int - total number of cores required by iterations pending in scheduling queue
        """
        return self.schedule_queue.waiting_cores

    def get_resource_usage(self):
        """Return current resource usage.

        Value:
            1.0 - all resources are used
            0 - no resources are used

        Returns:
            float - ratio of used cores to the total number of cores.
        """
        return self.get_used_cores() / self.get_total_cores()

    def get_planned_usage(self):
        """Return planned resource usage.

        Value:
            > 1 - enough waiting iterations to use all available resources
            0 - no waiting iterations

        Returns:
            float - ratio of total number of waiting iterations to the total available number of cores in allocation.
        """
        return self.get_pending_cores() / self.get_total_cores()


class EventType(Enum):

    # emited when execution of some iteration finished
    ITERATION_FINISHED = 1

    # emited when received signal from InputQueue about new queued jobs
    NEW_JOBS_IN_IQ = 2

    # emited when new iterations has been enqueued in schedule queue
    ENQUEUED_JOBS = 3

    # emited when executor starts
    INITIALIZED = 4


class Event:

    def __init__(self, type, data=None):
        """Scheduling event.

        Args:
            type (EventType) - event type
            data (Object) - optional event data
        """
        self.type = type
        self.data = data


class EXManager:

    def __init__(self, config):
        """Manager of executor instance.

        Attributes:
            name (str): manager instance name - under this name executor will register in
                input queue

        Args:
            config (dict): configuration
        """
        self.config = config
        self.name = self.config.get(Var.EXECUTOR_NAME)
        self.iq = IQInstance(self.config.get(Var.IQ_ADDRESS),
                             self.config.get(Var.IQ_KEY),
                             self.name,
                             self)

        self.resources = get_resources(self.config)

        self.jobs = Jobs()

        self.scheduler = Scheduler(self.resources)

        self.schedule_queue = ScheduleQueue(self.scheduler, self.execute_iteration, self.fail_iteration)

        self.executor = Executor(self.config, self.resources, self.iteration_started, self.iteration_finished)

        self.schedule_events_queue = asyncio.Queue()
        self.scheduler_task = None

        self.resource_usage = ResourceUsage(self.resources, self.schedule_queue)

    async def start(self):
        ExEvent.start(None).publish()

        await self.executor.start_services()

        await self.iq.register(self.resources.to_dict(nodetails=True))

        self.scheduler_task = asyncio.ensure_future(self.schedule_manager())

        self.signal_schedule_event(Event(EventType.INITIALIZED))

    async def stop(self):
        ExEvent.stop(None).publish()

        logging.info('stopping executor manager')

        if self.scheduler_task:
            try:
                self.scheduler_task.cancel()
            except Exception as exc:
                logging.warning(f'failed to cancel scheduler manager: {str(exc)}')

            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                logging.debug('scheduler manager canceled')
            self.scheduler_task = None

        await self.executor.stop_services()

        await self.iq.close()

    async def enqueue_new_jobs(self, jobs):
        """Register new jobs to schedule from InputQueue service."""
        schedule_jobs = list()
        for job_request in jobs:
            if not all(param in job_request for param in ['job', 'iterations']):
                logging.error(f'wrong job request {job_request} - missing job/iteration key parameter')

            try:
                job_description = JobDescription(**job_request.get('job'))

                job_iterations = JobIterations(job_description.name, job_description,
                                               job_request.get('attributes'), job_request.get('iterations'))
                logging.info('registering new job iterations')
                self.jobs.add(job_iterations)
                schedule_jobs.append(job_iterations)
            except:
                logging.exception('failed to enqueue job in executor')

        if len(schedule_jobs):
            ExEvent.enqueue_its({'jobs': schedule_jobs}).publish()

            logging.info(f'enqueing {len(schedule_jobs)} new jobs in schedule queue')
            await self.schedule_queue.enqueue(schedule_jobs)
            return True

        return False

    def execute_iteration(self, job_iterations, iteration, allocation):
        """Method called by scheduling queue when allocation will be find for specific job iteration.

        Args:
            job_iterations (JobIterations): job iteration to execute
            iteration (int,str): specific iteration to execute
            allocation (Allocation): allocation where to execute job iteration
        """
        ExEvent.execute_it({'job': job_iterations, 'iteration': iteration, 'allocation': allocation})
        asyncio.ensure_future(self._launch_iteration_execution(job_iterations, iteration, allocation))

    async def _launch_iteration_execution(self, job_iterations, iteration, allocation):
        try:
            job_iterations.set_state(iteration, JobState.SCHEDULED)
            await self.executor.execute(job_iterations, iteration, allocation)
        except Exception as exc:
            logging.exception(f'failed to execute iteration {job_iterations.id} ({job_iterations.name})')
            self.fail_iteration(job_iterations, iteration, f'failed to submit job: {str(exc)}')

    def fail_iteration(self, job_iterations, iteration, message=None):
        """Method called by scheduling queue when job iteration will never be executed.

         Args:
             job_iterations (JobIterations): job iteration that failed
             iteration (int,str): specific iteration that failed
             message (str): optionally cause message
         """
        ExEvent.finished_it({'job': job_iterations, 'iteration': iteration, 'state': JobState.FAILED})
        job_iterations.set_state(iteration, JobState.FAILED)
        self.iq.notify_finished_jobs([{'id': job_iterations.name, 'iteration': iteration, 'state': JobState.FAILED.name}])
        if job_iterations.all_finished():
            self.jobs.delete(job_iterations.id)

    def iteration_started(self, job_iterations, iteration, allocation):
        """Method called by executor when job iteration start's to execute"""
        ExEvent.started_it({'job': job_iterations, 'iteration': iteration})
        job_iterations.set_state(iteration, JobState.EXECUTING)

    def iteration_finished(self, job_iterations, iteration, allocation, exit_code, message=None):
        """Method called by executor when job iteration finished its execution.

        Args:
            job_iterations (JobIterations): job itreation that finished
            iteration (int, str): specific iteration that finished
            allocation (Allocation): allocation where released by job iteration
            exit_code (int): iteration's exit code
            message (str): optionally message
        """
        self.scheduler.release_allocation(allocation)

        finish_state = JobState.SUCCEED if exit_code == 0 else JobState.FAILED
        ExEvent.finished_it({'job': job_iterations, 'iteration': iteration, 'state': finish_state})
        job_iterations.set_state(iteration, finish_state)
        self.iq.notify_finished_jobs([{'id': job_iterations.name,
                                       'iteration': iteration,
                                       'state': finish_state.name,
                                       'message': message}])
        if job_iterations.all_finished():
            self.jobs.delete(job_iterations.id)

        self.signal_schedule_event(Event(EventType.ITERATION_FINISHED))

    def signal_schedule_event(self, event):
        """Emit new event related to the schedule event.

        Args:
            event (Event): event to emit
        """
        try:
            self.schedule_events_queue.put_nowait(event)
        except asyncio.QueueFull:
            logging.error(f'event queue is full ({self.schedule_events_queue.qsize()} - failed to emit '
                          f'event {event.type}')

    async def schedule_manager(self):
        """The method waits for signals about:
            * iteration finished
            * new jobs in input queue
        to:
            * request for new iterations from input queue to execute
            * run schedule loop
        """
        try:
            while True:
                event = await self.schedule_events_queue.get()
                self.schedule_events_queue.task_done()
                logging.info(f'received scheduling event {event.type}')

                ru = self.resource_usage
                logging.info(f'scheduling status: used cores ({ru.get_used_cores()} / {ru.get_total_cores()}), '
                             f'resource usage({ru.get_resource_usage()}), '
                             f'pending cores({ru.get_pending_cores()}), '
                             f'waiting usage({ru.get_planned_usage()})')

                if event.type == EventType.ITERATION_FINISHED:
                    if ru.get_resource_usage() < 1:
                        self.iq.request_new_iterations()

                    await self.schedule_queue.schedule_loop()
                elif event.type == EventType.NEW_JOBS_IN_IQ:
                    self.iq.request_new_iterations()
                elif event.type == EventType.ENQUEUED_JOBS:
                    jobs = event.data
                    if jobs:
                        try:
                            if await self.enqueue_new_jobs(jobs):
                                await self.schedule_queue.schedule_loop()
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logging.exception(f'failed to enqueue new jobs')
                elif event.type == EventType.INITIALIZED:
                    self.iq.request_new_iterations()

        except asyncio.CancelledError:
            raise
        finally:
            logging.info('finishing scheduler manager')
