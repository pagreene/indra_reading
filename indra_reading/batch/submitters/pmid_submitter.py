import boto3
import logging

from indra.util.aws import get_batch_command
from indra_reading.batch.submitters.reading_submitter import ReadingSubmitter
from indra_reading.batch.util import get_environment


logger = logging.getLogger('pmid_submitter')


class PmidSubmitter(ReadingSubmitter):
    _s3_input_name = 'pmids'
    _purpose = 'pmid_reading'
    _job_queue = 'run_reach_queue'
    _job_def_dict = {'run_reach_jobdef': ['reach', 'sparser'],
                     'run_db_reading_isi_jobdef': ['isi']}

    def _get_base(self, job_name, start_ix, end_ix):
        base = ['python', '-m', 'indra_reading.scripts.pmid_reading.read_pmids_aws',
                self.job_base, '/tmp', '16', str(start_ix), str(end_ix)]
        return base

    def _get_extensions(self):
        extensions = []
        for opt_key in ['force_read', 'force_fulltext']:
            if self.options.get(opt_key, False):
                extensions.append('--' + opt_key)
        return extensions

    def set_options(self, force_read=False, force_fulltext=False):
        """Set the options for this run."""
        self.options['force_read'] = force_read
        self.options['force_fulltext'] = force_fulltext
        return

    def submit_combine(self):
        job_ids = self.job_list
        if job_ids is not None and len(job_ids) > 20:
            print("WARNING: boto3 cannot support waiting for more than 20 jobs.")
            print("Please wait for the reading to finish, then run again with the")
            print("`combine` option.")
            return

        # Get environment variables
        environment_vars = get_environment()

        job_name = '%s_combine_reading_results' % self.job_base
        command_list = get_batch_command(
            ['python', '-m', 'indra_reading.scripts.assemble_reading_stmts_aws',
             self.job_base, '-r'] + self.readers,
            purpose='pmid_reading',
            project=self.project_name
        )
        logger.info('Command list: %s' % str(command_list))
        kwargs = {'jobName': job_name, 'jobQueue': self._job_queue,
                  'jobDefinition': 'run_reach_jobdef',
                  'containerOverrides': {'environment': environment_vars,
                                         'command': command_list,
                                         'memory': 60000, 'vcpus': 1}}
        if job_ids:
            kwargs['dependsOn'] = job_ids
        batch_client = boto3.client('batch')
        batch_client.submit_job(**kwargs)
        logger.info("submitted...")
        return


