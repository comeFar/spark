import zerorpc
from config import Config
import gevent
import time


class Master(object):
    progress = ""
    all_workers = set()
    workers_info = {}  # "id" : None, "current_job" : None, "compeleted_jobs": set(), "last_received_hb": xxx

    busy_workers = set()
    idle_workers = set()

    def __init__(self):
        self.reset_for_new_job()
        # gevent.spawn(self.controller)
        gevent.spawn(self.validate_workers)
        pass

    def reset_for_new_job(self):
        Master.progress = ""

    def process_heart_beat(self, ip_address, status, job_id, job_type):
        Master.progress = self.get_printable_status(status)
        print"[Heart_beat_Recieved from {0} ....  Status - {1}, Job_type - {2}, Job_ID - {3}......... ] \n".format(
            ip_address, self.get_printable_status(status), self.get_printable_status(job_type), job_id)
        self.register_worker(ip_address)

    def get_printable_status(self, status):
        if status == Config.WORKER_STATUS_REDUCE_FAILED:
            return "Failed"
        elif status == Config.WORKER_STATUS_IDLE:
            return "Idle"
        elif status == Config.WORKER_STATUS_COMPLETE:
            return "Completed"
        elif status == Config.WORKER_STATUS_WORKING_MAP:
            return "MapTask"
        elif status == Config.WORKER_STATUS_WORKING_REDUCE:
            return "ReduceTask"
        elif status == Config.WORKER_STATUS_WORKING_SHUFFLE:
            return "ShuffleTask"
        else:
            return str(status)

    def reset_failed_worker_job(self, worker_id):
        pass

    def update_completed_jobs(self, ip_address, job_id):
        pass

    def validate_workers(self):
        # Check for worker heart beat Intervals.
        # Unregister invalid workers
        # reassign the job to the pending job pool
        while True:
            cur_time = time.time()
            for key in Master.workers_info.keys():
                value = Master.workers_info[key]
                last_time = value['last_received_hb']
                if cur_time - last_time > (Config.HEART_BEAT_TIME_INTREVAL * 3 + 1):
                    print '******************** Worker {0} is down ********************'.format(value['id'])
                    self.unregister_worker(key)

            gevent.sleep(Config.HEART_BEAT_TIME_INTREVAL)

    def register_worker(self, ip_address):
        cur_time = time.time()
        if ip_address in Master.workers_info:
            Master.workers_info[ip_address]['last_received_hb'] = cur_time
            return
        Master.workers_info[ip_address] = {"id": ip_address, "current_map_job": None, "compeleted_jobs": set(),
                                           "last_received_hb": cur_time, "current_reduce_job": None}
        Master.all_workers.add(ip_address)
        self.set_worker_idle(ip_address)
        print "[New worker {0} Registered .... ]".format(ip_address)

    def unregister_worker(self, ip_address):
        self.reset_failed_worker_job(ip_address)
        del Master.workers_info[ip_address]
        Master.all_workers.discard(ip_address)

    def start_job(self):
        worker_id = Master.idle_workers.pop()
        job_name = "Test Job"
        input_file = ""
        start_index = 0
        chunk_size = 0
        c = zerorpc.Client()
        c.connect(worker_id)
        print "[......... Starting {1} Job On {0} ..... (Map Phase) ......] \n".format(worker_id, job_name)

        if c.start_job(job_name, input_file, start_index, chunk_size, Master.no_of_reducers):
            print "[......... {1} Job started successfully On  {0} ..... Job_id - {2} (Map Phase).......] \n".format(
                worker_id, job_name, start_index)
            Master.workers_info[worker_id]["current_map_job"] = start_index
            self.set_worker_busy(worker_id)
        else:
            print "[.............. Unable to Start {1} Job On  {0} ..... (Map Phase)..........] \n".format(worker_id,
                                                                                                           job_name)
            Master.workers_info[worker_id]["current_map_job"] = None
            self.set_worker_idle(worker_id)

        c.close()

    def set_worker_busy(self, ip_address):
        Master.idle_workers.discard(ip_address)
        Master.busy_workers.add(ip_address)

    def set_worker_idle(self, ip_address):
        Master.busy_workers.discard(ip_address)
        Master.idle_workers.add(ip_address)
        Master.workers_info[ip_address]["current_reduce_job"] = None
        Master.workers_info[ip_address]["current_map_job"] = None

    def split_input(self, input_file, chunk_size):
        # return splitter.split_file(input_file, chunk_size)
        pass

    def submit_job(self, job_name, input_file, chunk_size):
        self.reset_for_new_job()
        Master.progress = "Submiting Job ....."
        Master.job_name = job_name
        Master.input_file = input_file
        Master.chunk_size = chunk_size
        # Master.split_map_jobs = self.split_input(input_file, chunk_size)
        # Master.pending_map_jobs = set(Master.split_map_jobs)
        # Master.pending_reduce_jobs = set(list(range(0, no_of_reducers)))
        print "[{0} Job Submmited ..... ]".format(job_name)

    def get_job_id(self, job):
        return Master.split_map_jobs.index(job)

    def get_other_workers(self, ip_address):
        other_workers = []
        for worker_id in Master.all_workers:
            if worker_id != ip_address:
                other_workers.append(worker_id)
        return other_workers

    def progress_update(self):

        return "Completed"


s = zerorpc.Server(Master())
print(s.bind(Config.MASTER_IP))
s.run()
