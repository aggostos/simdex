from interfaces import AbstractDispatcher
from jobs import JobDurationIndex

def duration_filter(est_duration):
    def fnc(w):
        limit = w.get_attribute("limit")
        return limit is None or limit >= est_duration
    return fnc


class OracleJobCategoryDispatcher(AbstractDispatcher):
    """Same as JobCategoryDispatcher but with oracle that precisely forsees the job duration.

    This dispatcher can be used to determine the performance of theoretical ultimate model that would
    predict job durations precisely.
    """

    def __init__(self):
        # the dispatching algorithm only reads the index, SA strategy is responsible for filling the data
        self.duration_index = JobDurationIndex()

    def init(self, ts, workers):
        pass

    def dispatch(self, job, workers):
        # the dispatcher is cheating here, the duration would not be available until the job is completed !!!
        q_len_estimates = []
        for i, worker in enumerate(workers):
            estimate = job.duration / worker.get_attribute("performance") #if i != 3 else job.duration / (worker.get_attribute("performance") + 2)
            
            q_len_estimate = 0
            for q_job in worker.jobs:
                q_len_estimate += q_job.duration / worker.get_attribute("performance") #if i != 3 else q_job.duration / (worker.get_attribute("performance") + 2)
            
            q_len_estimates.append((worker, q_len_estimate + estimate))


        q_len_estimates.sort(key=lambda k: k[1])
        target = q_len_estimates[0][0]
        target.enqueue(job)

    def update_performances(self, perfs):        
        pass

    def add_ref_job(self, job):
        """External interface for SA strategy (which can add jobs to index to modify dispatcher behavior)."""
        self.duration_index.add(job)

class JobCategoryDispatcher(AbstractDispatcher):
    """Same as JobCategoryDispatcher but with oracle that precisely forsees the job duration.

    This dispatcher can be used to determine the performance of theoretical ultimate model that would
    predict job durations precisely.
    """

    def __init__(self):
        # the dispatching algorithm only reads the index, SA strategy is responsible for filling the data
        self.duration_index = JobDurationIndex()
        self.perfs = [1] * 4

    def init(self, ts, workers):
        pass

    def dispatch(self, job, workers):
        # the dispatcher is cheating here, the duration would not be available until the job is completed !!!
        estimate = self.duration_index.estimate_duration(job.exercise_id, job.runtime_id)
        if estimate is None:
            estimate = job.limits / 2.0

        # select workers where the job would fit (estimate duration is under worker limit)
        best_workers = list(filter(duration_filter(estimate), workers))
        if len(best_workers) == 0:
            best_workers = workers  # fallback, if no worker passes the limit

        q_len_estimates = []
        for i, worker in enumerate(best_workers):
            w_estimate = estimate / self.perfs[i]
            
            q_len_estimate = 0
            
            for q_job in worker.jobs:
                q_job_estimate = self.duration_index.estimate_duration(q_job.exercise_id, q_job.runtime_id)
                if q_job_estimate is None:
                    q_job_estimate = q_job.limits / 2.0
                q_len_estimate += q_job_estimate / self.perfs[i]
            
            q_len_estimates.append((worker, q_len_estimate + w_estimate))


        q_len_estimates.sort(key=lambda k: k[1])
        target = q_len_estimates[0][0]
        target.enqueue(job)

    def update_performances(self, perfs):        
        self.perfs = perfs

    def add_ref_job(self, job):
        """External interface for SA strategy (which can add jobs to index to modify dispatcher behavior)."""
        self.duration_index.add(job)


class BadJobCategoryDispatcher(AbstractDispatcher):
    """Same as JobCategoryDispatcher but with oracle that precisely forsees the job duration.

    This dispatcher can be used to determine the performance of theoretical ultimate model that would
    predict job durations precisely.
    """

    def __init__(self):
        # the dispatching algorithm only reads the index, SA strategy is responsible for filling the data
        self.duration_index = JobDurationIndex()

    def init(self, ts, workers):
        pass

    def dispatch(self, job, workers):
        # the dispatcher is cheating here, the duration would not be available until the job is completed !!!
        estimate = self.duration_index.estimate_duration(job.exercise_id, job.runtime_id)
        if estimate is None:
            estimate = job.limits / 2.0

        q_len_estimates = []
        for i, worker in enumerate(workers):
            w_estimate = estimate
            
            q_len_estimate = 0
            
            for q_job in worker.jobs:
                q_job_estimate = self.duration_index.estimate_duration(q_job.exercise_id, q_job.runtime_id)
                if q_job_estimate is None:
                    q_job_estimate = q_job.limits / 2.0
                q_len_estimate += q_job_estimate

            q_len_estimates.append((worker, q_len_estimate + w_estimate))

        q_len_estimates.sort(key=lambda k: k[1])
        target = q_len_estimates[0][0]
        target.enqueue(job)

    def update_performances(self, perfs):        
        pass

    def add_ref_job(self, job):
        """External interface for SA strategy (which can add jobs to index to modify dispatcher behavior)."""
        self.duration_index.add(job)