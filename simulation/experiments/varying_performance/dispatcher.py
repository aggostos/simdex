from interfaces import AbstractDispatcher
from jobs import JobDurationIndex


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
        for worker in workers:
            estimate = job.duration / worker.get_attribute("performance")
            
            q_len_estimate = 0
            for q_job in worker.jobs:
                q_len_estimate += q_job.duration / worker.get_attribute("performance")
            
            q_len_estimates.append((worker, q_len_estimate + estimate))


        q_len_estimates.sort(key=lambda k: k[1])
        target = q_len_estimates[0][0]
        target.enqueue(job)

    def add_ref_job(self, job):
        """External interface for SA strategy (which can add jobs to index to modify dispatcher behavior)."""
        self.duration_index.add(job)
