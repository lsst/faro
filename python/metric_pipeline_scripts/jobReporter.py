import numpy as np

from lsst.verify import Job, MetricSet
from lsst.daf.butler import Butler
class JobReporter:
    def __init__(self, repository, collection, metrics_package, spec):
        self.metrics = MetricSet.load_metrics_package(package_name_or_path='verify_metrics',  # Maybe allow an argument
                                                      subset=metrics_package)
        self.butler = Butler(repository)
        self.registry = self.butler.registry
        self.spec = spec
        self.collection = collection
    def run(self):
        jobs = {}
        for metric in self.metrics:
            data_ids = list(self.registry.queryDatasets(f'metricvalue_summary_{metric.package}_{metric.metric}',
                            collections=self.collection))
            for did in data_ids:
                m = self.butler.get(did, collections=self.collection)
                if np.isnan(m.quantity):
                    continue
                # make the name the same as what SQuaSH Expects
                m.metric_name = metric
                tract = did.dataId['tract']
                filt = did.dataId['abstract_filter']
                key = f"{tract}_{filt}"
                if key not in jobs.keys():
                    job_metadata = {'instrument': did.dataId['instrument'],
                         'filter_name': filt,
                         'tract': tract}
                         # Get this from repository? 'dataset_repo_url': dataset_repo_url})
                    jobs[key] = Job(meta=job_metadata, metrics=self.metrics)
                jobs[key].measurements.insert(m)
        return jobs
