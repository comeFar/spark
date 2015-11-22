import zerorpc
import gevent
from config import Config
import os, errno
import sys


class Worker(object):
    # no_of_reducers = 0
    # completed_map_jobs = {}
    # completed_reduce_jobs = set()
    shuffled_data = {}
    # engine = None
    job_name = None
    job_type = None

    master_ip = ""

    status = Config.WORKER_STATUS_IDLE

    c = zerorpc.Client()

    def get_printable_status (self, status):
        if status == Config.WORKER_STATUS_REDUCE_FAILED:
            return "FAILED"
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

    def __init__(self, master_ip):
        Worker.master_ip = master_ip
        self.my_ip = sys.argv[1]
        Worker.c.connect(Worker.master_ip)
        gevent.spawn(self.heart_beat)

    def heart_beat(self):
        while True:
            self.send_heart_beat(None)
            gevent.sleep(Config.HEART_BEAT_TIME_INTREVAL)

    def send_heart_beat(self, job_id):
        print("[hear_beat_send .... Job_ID - {0} : {1}] ...").format(job_id, self.get_printable_status(Worker.status))
        Worker.c.process_heart_beat(self.my_ip, Worker.status, job_id, Worker.job_type)
        # reset the status of the worker after completed or failed task to idle

    def start_job(self, job_name, input_file, start_index, chunk_size, no_of_reducers):
        if Worker.job_name != job_name:
            self.reset_worker()
        Worker.job_name = job_name
        Worker.no_of_reducers = no_of_reducers
        # if(job_name == Jobs.WORD_COUNT_JOB):
        gevent.spawn(self.do_job, job_name, input_file, start_index, chunk_size, no_of_reducers)
        return True

    def do_job(self):
        for i in range(0, 10):
            print("Working on some task")
            gevent.sleep(2)
        self.process_completed_job(0, "")

    def process_completed_job(self, job_id, map_output):
        Worker.status = Config.WORKER_STATUS_COMPLETE
        Worker.job_type = Config.WORKER_STATUS_WORKING_MAP
        Worker.completed_map_jobs[job_id] = map_output
        gevent.spawn(self.send_heart_beat, job_id)


    def shuffle_info(self, reducer_id):
        map_data = {}
        for job_id in Worker.completed_map_jobs.keys():
            values = Worker.completed_map_jobs[job_id][reducer_id]
            map_data = self.shuffle_merge(map_data, values)
        return map_data


    def get_remote_shuffle_data(self, reducer_id, other_worker_id):
        worker_connect = zerorpc.Client()
        worker_connect.connect(other_worker_id)
        shuffle_data = worker_connect.shuffle_info(reducer_id)
        worker_connect.close()
        return shuffle_data

    def shuffle_phase(self, reducer_id, other_workers):
        Worker.status = Config.WORKER_STATUS_WORKING_SHUFFLE
        temp_shuffle_data = {}
        Worker.shuffled_data = {}
        for worker_id in other_workers:
            temp_shuffle_data = self.shuffle_merge(temp_shuffle_data,
                                                      self.get_remote_shuffle_data(reducer_id, worker_id))
        # get local shuffle data
        local_data = self.shuffle_info(reducer_id)
        temp_shuffle_data = self.shuffle_merge(temp_shuffle_data, local_data)
        return temp_shuffle_data

    def shuffle_merge(self, data_1, data_2):
        for k in data_2.keys():
            if k in data_1:
                new_list = data_1[k] + data_2[k]
                data_1[k] = new_list
            else:
                data_1[k] = data_2[k]
            gevent.sleep(Config.MIN_TIME_TO_PREEMPT_TASK)
        return data_1

    def silentremove(self, filename):
        try:
            os.remove(filename)
        except OSError as e:  # this would be "except OSError, e:" before Python 2.6
            # if e.errno != errno.ENOENT: # errno.ENOENT = no such file or directory
            pass  # re-raise exception if a different error occured


    def reset_worker(self):
        Worker.no_of_reducers = 0
        Worker.completed_map_jobs = {}
        Worker.completed_reduce_jobs = set()
        Worker.shuffled_data = {}
        Worker.engine = None
        Worker.job_name = None
        Worker.job_type = None
        print "............... Worker Reset Complete ....................."

s = zerorpc.Server(Worker(Config.MASTER_IP))
print(s.bind(sys.argv[1]))
s.run()
