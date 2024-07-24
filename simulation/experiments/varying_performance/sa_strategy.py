from interfaces import AbstractSelfAdaptingStrategy


class CategorySelfAdaptingStrategy(AbstractSelfAdaptingStrategy):
    """Represents a SA controller that uses simple machine learning.

    Collects job and ref. job metadata to compute categorized statistics of job duration based on their
    affiliation to exercises and runtimes. These statistics are used by dispatcher for predicting the duration
    of incomming jobs.
    """

    def __init__(self, max_long_queues, ref_jobs):
        self.max_long_queues = max_long_queues
        self.ref_jobs = ref_jobs[:]
        self.ref_jobs.reverse()
        self.perfs = [1] * 4

    def _update_dispatcher(self, ts, dispatcher):
        while len(self.ref_jobs) > 0 and self.ref_jobs[-1].spawn_ts + self.ref_jobs[-1].duration <= ts:
            job = self.ref_jobs.pop()
            if job.compilation_ok:
                dispatcher.add_ref_job(job)

    def init(self, ts, dispatcher, workers):
        self._update_dispatcher(ts, dispatcher)

    def do_adapt(self, ts, dispatcher, workers, job=None):
        self._update_dispatcher(ts, dispatcher)        
        if (job and job.compilation_ok):            
            dispatcher.add_ref_job(job)

            estimate = dispatcher.duration_index.estimate_duration(job.exercise_id, job.runtime_id)
            actual = job.finish_ts - job.start_ts

            alpha = 0.9     
            if self.perfs[job.worker_id] == 1:
                self.perfs[job.worker_id] = (estimate / actual)
            else:
                self.perfs[job.worker_id] = self.perfs[job.worker_id] * alpha + (estimate / actual) * (1 - alpha)

            best = sorted(zip(range(4), self.perfs), key=lambda x: x[1], reverse=True)            

            for i, w in enumerate(workers):
                if i == best[3][0]:
                    w.set_attribute("limit", 30)
                    continue
                w.set_attribute("limit", 10000)
            
            #print(self.perfs)
            dispatcher.update_performances(self.perfs)
