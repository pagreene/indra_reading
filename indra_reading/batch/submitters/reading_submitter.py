import logging

import boto3

from indra_reading.batch.util import bucket_name
from indra_reading.readers import get_reader_classes
from indra_reading.batch.submitters.submitter import Submitter

logger = logging.getLogger('reading_submitter')


class ReadingSubmitter(Submitter):
    """A further development on Submitter for the purposes of bulk reading.

    Reading submitters, broadly, will accept both a basename *and* a list of
    readers to be read. Furthermore, an input file with content IDs must be
    provided to the run method, followed by the number of ids_per_job.

    The start index and stop index of rows in the file may also be provided
    between the input file and the ids per job, for the sake of backwards
    compatibility.
    """
    _s3_input_name = NotImplemented
    job_class = 'reading'

    def __init__(self, basename, readers, *args, **kwargs):
        if 'all' in readers:
            self.readers = [rc.name.lower() for rc in get_reader_classes()]
        else:
            self.readers = readers
        self.ids_per_job = None
        super(ReadingSubmitter, self).__init__(basename, *args, **kwargs)

    def _iter_over_select_queues(self):
        for queue_name, job_type_list in self._job_queue_dict.items():
            if not any(reader in job_type_list for reader in self.readers):
                logger.info("Queue %s will not be used, no relevant readers "
                            "selected." % queue_name)
                continue
            yield queue_name

    def _get_command(self, job_type_set, *args):
        start_ix, end_ix = args
        reader_list = [r for r in job_type_set if r in self.readers]
        if not reader_list:
            return None, None

        # Build the command.
        job_name = '%s_%d_%d' % (self.job_base, start_ix, end_ix)
        job_name += '_' + '_'.join(reader_list)
        cmd = self._get_base(job_name, start_ix, end_ix)
        cmd += ['-r'] + reader_list
        cmd += self._get_extensions()
        return job_name, cmd

    def _iter_job_args(self, *args):
        """Iterate over the relevant arguments for a batch of reading jobs

        Parameters
        ----------
        input_fname : str
            The name of the file containing the ids to be read.
        start_ix : int
            (optional) The line index of the first item in the list to read.
        end_ix : int
            (optional) The line index of the last item in the list to be read.
        ids_per_job : int
            The number of ids to be given to each job.
        """
        # Parse the args. We need to handle the case where start_ix and end_ix
        # are used for backwards compatibility.
        if len(args) == 2:
            input_fname, ids_per_job = args
            start_ix = None
            end_ix = None
        elif len(args) == 4:
            input_fname, start_ix, end_ix, ids_per_job = args
        else:
            raise TypeError(f'_iter_job_args() takes 2 or 4 positional '
                            f'arguments, but {len(args)} were given')

        # stash this for later.
        self.ids_per_job = ids_per_job

        # Upload the pmid_list to Amazon S3
        id_list_key = self.s3_base + self._s3_input_name
        s3_client = boto3.client('s3')
        s3_client.upload_file(input_fname, bucket_name, id_list_key)

        # If no end index is specified, read all the PMIDs
        if end_ix is None:
            with open(input_fname, 'rt') as f:
                lines = f.readlines()
                end_ix = len(lines)

        if start_ix is None:
            start_ix = 0

        for job_start_ix in range(start_ix, end_ix, ids_per_job):

            job_end_ix = job_start_ix + ids_per_job
            if job_end_ix > end_ix:
                job_end_ix = end_ix

            yield job_start_ix, job_end_ix

    def submit_reading(self, input_fname, start_ix, end_ix, ids_per_job,
                       num_tries=1, stagger=0):
        """Submit a batch of reading jobs.

        This is now just a thin wrapper around `submit_jobs`.


        Parameters
        ----------
        input_fname : str
            The name of the file containing the ids to be read.
        start_ix : int
            (optional) The line index of the first item in the list to read.
        end_ix : int
            (optional) The line index of the last item in the list to be read.
        ids_per_job : int
            The number of ids to be given to each job.
        num_tries : int
            The number of times a job may be attempted.
        stagger : float
            The number of seconds to wait between job submissions.

        Returns
        -------
        job_lists : dict{queue_name: list[str]}
            A dict of lists of job id strings, keyed by the name of each queue
            used.
        """
        return self.submit_jobs(input_fname, start_ix, end_ix, ids_per_job,
                                num_tries=num_tries, stagger=stagger)

    def run(self, input_fname, ids_per_job, stagger=0, **wait_params):
        """Run this submission all the way.

        This method will run both `submit_jobs` and `watch_and_wait`,
        blocking on the latter.
        """
        super(ReadingSubmitter, self).run(input_fname, ids_per_job,
                                          stagger=stagger, **wait_params)

    def _get_base(self, job_name, start_ix, end_ix):
        raise NotImplementedError

    def _get_extensions(self):
        return []

