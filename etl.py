from ctypes import c_int
from tqdm import tqdm
from typing import Callable

import multiprocessing as mp


class Reset:
    """This is an empty class for state reseting."""
    None


class WorkCluster:
    processes = []

    def __init__(self,
                 name: str,
                 target: Callable,
                 kwargs: dict,
                 in_var: str,
                 num_processes=4):
        """Initialise a new WorkCluster instance.

        name: [str] The name prefix of each process to be created.
        target: [Callable] A function to be run on the values in the task
            queue.
        kwargs: [dict] A dictionary of arguments to be passed alongside the task
            queue values to target.
        in_var: [str] The name of the argument that the task queue values will
            take in target.
        num_processes: [int] The number of processes to spawn.
        """
        # Arguments are set to the object.
        self.name = name
        self.target = target
        self.kwargs = kwargs
        self.in_var = in_var

        # We initiate the pool
        self.pool = mp.Pool(processes=num_processes)

        # The task (in) queue is created
        self.tasks_manager = mp.Manager()
        self.tasks = self.tasks_manager.Queue()
        self.tasks_lock = self.tasks_manager.Lock()

        # The result (out) queue is created
        self.results_manager = mp.Manager()
        self.results = self.results_manager.Queue()
        self.results_lock = self.results_manager.Lock()

        # Variables for insight into what the cluster is doing are allocated
        self.finished_tasks = mp.Value(c_int)
        self.finished_tasks.value = 0
        self.num_processes = num_processes
        self.finished_processes = mp.Value(c_int)
        self.finished_processes.value = 0

        # The number of running processes is set:
        self. processes = []
        self.active_processes = num_processes
        # Initiate the worker processes
        for i in range(self.num_processes):
            # Set process name
            process_name = '{}{}'.format(name, i)
            # Create the process, and connect it to the worker function
            new_process = mp.Process(target=self.run)
            new_process.name = process_name
            # Add new process to the list of processes
            self.processes.append(new_process)
            # Start the process
            new_process.start()
            #new_process.join()
        print('{} initialised with {} processes.'.format(self.name,
                                                         self.num_processes))

    def poison(self):
        """None types are used as poison pills for workers. Note this could
            be problematic for untyped outputs such as functions."""
        self.tasks.put(None)
        print('{} was poisoned'.format(self.name))

    def put_task(self, task):
        """Safely put a task into the task queue."""
        self.tasks_lock.acquire()
        self.tasks.put(task)
        self.tasks_lock.release()

    def get_result(self):
        """Safely put a task into the task queue."""
        self.results_lock.acquire()
        result = self.results.get()
        self.results_lock.release()
        return result

    def run(self):
        """A loop to continually update and run processes on queued objects."""
        print('{} began a process'.format(self.name))
        while True:
            try:
                #self.tasks_lock.acquire()
                #new_value = self.tasks.get()
                #self.tasks_lock.release()
                lock = self.tasks_manager.Lock()
                lock.acquire()
                new_value = self.tasks.get()
                lock.release()
                print('{} acquired value:\n{}\n'.format(self.name, new_value))
            except EOFError:
                # There are no more items left in the stack so we bail.
                break
            if isinstance(new_value, type(None)):
                # Indicate finished
                print('{} killed a process'.format(self.name))
                self.results.put(None)
                self.finished_processes.value += 1
                break
            else:
                result = self.target(**{self.in_var: new_value, **self.kwargs})
                print('{} result value:\n{}\n'.format(self.name, result))
                #self.results_lock.acquire()
                #elf.results.put(result)
                #self.results_lock.release()
                lock = self.results_manager.Lock()
                lock.acquire()
                self.results.put(result)
                lock.release()
                with self.finished_tasks.get_lock():
                    self.finished_tasks.value += 1
        return


def etl(task_list: list,
        extract: Callable,
        transform: Callable,
        load: Callable,
        extract_kwargs={},
        transform_kwargs={},
        load_kwargs={},
        extract_in_var='input',
        transform_in_var='input',
        load_in_var='input'):
    """Asynchronously extract, transform and load data.

    task_list: [list] The list of tasks to be completed, for example a list of
        queries to be processed to the EDW.
    extract: [function] The initial function to be executed on the task list.
    transform: [function] The function that is executed on the results from the
        extract function.
    load: [function] The function that is executed on the results from the
        transform function.
    extract_kwargs: [dict] A dictionary of additional named arguments to be
        passed to the extract function.
    transform_kwargs: [dict] A dictionary of additional named arguments to be
        passed to the transform function.
    load_kwargs: [dict] A dictionary of additional named arguments to be
        passed to the load function.
    extract_in_var: [str] The name of the variable that will be taken off of
        the task list queue and passed to the extractor function.
    transform_in_var: [str] The name of the variable that will be taken off of
        the extractors results queue and passed to the transform function.
    load_in_var: [str] The name of the variable that will be taken off of
        the transformers results queue and passed to the load function.
    """
    ###########
    # Extract #
    ###########
    extractor = WorkCluster(name='extractor',
                            target=extract,
                            kwargs=extract_kwargs,
                            in_var=extract_in_var)

    # Fill task queue
    for task in task_list:
        extractor.tasks.put(task)
    # Quit the worker processes by sending them None after all the tasks are
    # completed
    for i in range(extractor.num_processes):
        extractor.tasks.put(None)

    #############
    # Transform #
    #############
    transformer = WorkCluster(name='transformer',
                              target=transform,
                              kwargs=transform_kwargs,
                              in_var=transform_in_var)

    ########
    # Load #
    ########
    loader = WorkCluster(name='loader',
                         target=load,
                         kwargs=load_kwargs,
                         in_var=load_in_var)

    # Read calculation results
    num_finished_tasks_o = mp.Value(c_int)
    num_tasks = len(task_list)*3

    # A queue for finished items is created. This is to confirm that a task is
    # complete.
    complete_manager = mp.Manager()
    complete_tasks = complete_manager.Queue()
    complete_lock = complete_manager.Lock()

    print('Task list and work clusteres ready, now beginning!')
    t = tqdm(total=num_tasks)
    while not (num_finished_tasks_o.value) == (num_tasks):
        # Make sure that the queue has something to take off
        if extractor.results.empty():
            pass
        else:
            #extract_new_result = extractor.get_result()
            extract_new_result = extractor.results.get()
            # Have a look at the results
            if isinstance(extract_new_result, type(None)):
                # An extract process has finished, so poisoned the transformer
                transformer.poison()
            else:
                # If the result is something else, we add it to the queue
                # Add task for transformation.
                #transformer.put_task(extract_new_result)
                transformer.tasks.put(extract_new_result)

        # Make sure that the queue has something to take off
        if transformer.results.empty():
            pass
        else: 
            transform_new_result = transformer.get_result()
            if isinstance(transform_new_result, type(None)):
                # Transform process has finished add a poison pill to loader
                loader.poison()
            else:
                # otherwise the result is sent to the loader
                #loader.put_task(transform_new_result)
                loader.tasks.put(transform_new_result)

        # To make sure that the loaders queues are still processed accordingly
        if loader.results.empty():
            pass
        else:
            #finished_task = loader.get_result()
            finished_task = loader.results.get()

            # The result is sent to a queue representing a stack of completed
            # tasks. This is not entirely necessary but is useful for debugging
            # code behaviour.
            complete_lock.acquire()
            complete_tasks.put(finished_task)
            complete_lock.release()

        # The loading bar is updated at every interval, whether a task was
        # completed or not.
        t.update(num_finished_tasks_o.value)

    # We make sure the final tasks are loaded as they may hang in the loader
    """
    if loader.tasks.empty():
        pass
    else:
        # If the tasks are not loaded we repoison the queue
        for i in range(loader.num_processes):
            loader.poison()
        # The queue is the flushed.
        while not loader.tasks.empty():
            loader.get_task()
        while not loader.results.empty():
            loader.get_results()
        print('tasks complete: {}'.format(num_finished_tasks_o.value))
    """
    loader.results_manager

    # Garbage collection
    extractor.pool.terminate()
    transformer.pool.terminate()
    loader.pool.terminate()

    extractor.pool = None
    transformer.pool = None
    loader.pool = None

    print('loader tasks empty is {}'.format(loader.tasks.empty()))
    print('loader results empty is {}'.format(loader.results.empty()))
    for i in range(len(loader.processes)):
        #loader.processes[i] = None
        loader.processes[i].terminate()
        print('process {} alive is {}'.format(loader.processes[i].name, loader.processes[i].is_alive()))

    print('transformer tasks empty is {}'.format(transformer.tasks.empty()))
    print('transformer results empty is {}'.format(transformer.results.empty()))
    for i in range(len(transformer.processes)):
        transformer.processes[i].terminate()
        print('transformer {} alive is {}'.format(transformer.processes[i].name, transformer.processes[i].is_alive()))

    print('extractor tasks empty is {}'.format(extractor.tasks.empty()))
    for i in range(len(extractor.processes)):
        extractor.processes[i].terminate()
        print('extractor {} alive is {}'.format(extractor.processes[i].name, extractor.processes[i].is_alive()))