import argparse

from indra_reading.batch.submitters.pmid_submitter import PmidSubmitter
from indra_reading.readers import get_reader_classes


def submit_reading(basename, pmid_list_filename, readers, start_ix=None,
                   end_ix=None, pmids_per_job=3000, num_tries=2,
                   force_read=False, force_fulltext=False, project_name=None):
    """Submit an old-style pmid-centered no-database s3 only reading job.

    This function is provided for the sake of backward compatibility. It is
    preferred that you use the object-oriented PmidSubmitter and the
    submit_reading job going forward.
    """
    sub = PmidSubmitter(basename, readers, project_name)
    sub.set_options(force_read, force_fulltext)
    sub.submit_reading(pmid_list_filename, start_ix, end_ix, pmids_per_job,
                       num_tries)
    return sub.job_list


def submit_combine(basename, readers, job_ids=None, project_name=None):
    """Submit a batch job to combine the outputs of a reading job.

    This function is provided for backwards compatibility. You should use the
    PmidSubmitter and submit_combine methods.
    """
    sub = PmidSubmitter(basename, readers, project_name)
    sub.job_list = job_ids
    sub.submit_combine()
    return sub


def create_submit_parser():
    import argparse
    parent_submit_parser = argparse.ArgumentParser(add_help=False)
    parent_submit_parser.add_argument(
        'basename',
        help='Defines job names and S3 keys'
    )
    parent_submit_parser.add_argument(
        '--group_name',
        help="Indicate what group of jobs this batch is a part of."
    )
    parent_submit_parser.add_argument(
        '-r', '--readers',
        dest='readers',
        choices=[rc.name.lower() for rc in get_reader_classes()] + ['all'],
        default=['all'],
        nargs='+',
        help='Choose which reader(s) to use.'
    )
    parent_submit_parser.add_argument(
        '--project',
        help=('Set the project name. Default is DEFAULT_AWS_PROJECT in the '
              'config.')
    )
    return parent_submit_parser


def create_read_parser():
    import argparse
    parent_read_parser = argparse.ArgumentParser(add_help=False)
    parent_read_parser.add_argument(
        'input_file',
        help=('Path to file containing input ids of content to read. For the '
              'no-db options, this is simply a file with each line being a '
              'pmid. For the with-db options, this is a file where each line '
              'is of the form \'<id type>:<id>\', for example \'pmid:12345\'')
    )
    parent_read_parser.add_argument(
        '--start_ix',
        type=int,
        help='Start index of ids to read.'
    )
    parent_read_parser.add_argument(
        '--end_ix',
        type=int,
        help='End index of ids to read. If `None`, read content from all ids.'
    )
    parent_read_parser.add_argument(
        '--force_read',
        action='store_true',
        help='Read papers even if previously read by current REACH.'
    )
    parent_read_parser.add_argument(
        '--force_fulltext',
        action='store_true',
        help='Get full text content even if content already on S3.'
    )
    parent_read_parser.add_argument(
        '--ids_per_job',
        default=3000,
        type=int,
        help='Number of PMIDs to read for each AWS Batch job.'
    )
    parent_read_parser.add_argument(
        '--stagger',
        default=0,
        type=int,
        help="Set the amount of time to wait between job submissions in secs."
    )
    ''' Not currently supported.
    parent_read_parser.add_argument(
        '--num_tries',
        default=2,
        type=int,
        help='Maximum number of times to try running job.'
        )
    '''
    return parent_read_parser


def create_parser():
    parent_submit_parser = create_submit_parser()
    parent_read_parser = create_read_parser()

    # Make non_db_parser and get subparsers
    parser = argparse.ArgumentParser(
        'indra_reading.scripts.submit_reading_pipeline.py',
        description=('Run reading by collecting content, and save as pickles. '
                     'This option requires that ids are given as a list of '
                     'pmids, one line per pmid.'),
        epilog=('Note that `python wait_for_complete.py ...` should be run as '
                'soon as this command completes successfully. For more '
                'details use `python wait_for_complete.py -h`.')
    )
    subparsers = parser.add_subparsers(
        title='Job Type',
        help='Type of jobs to submit.'
    )
    subparsers.required = True
    subparsers.dest = 'job_type'

    # Create subparsers for the no-db option.
    subparsers.add_parser(
        'read',
        parents=[parent_read_parser, parent_submit_parser],
        help='Run reading on AWS batch and cache INDRA Statements on S3.',
        description='Run reading on batch and cache INDRA Statements on S3.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    subparsers.add_parser(
        'combine',
        parents=[parent_submit_parser],
        help='Combine INDRA Statement subsets into a single file.',
        description='Combine INDRA Statement subsets into a single file.'
    )
    subparsers.add_parser(
        'full',
        parents=[parent_read_parser, parent_submit_parser],
        help='Run reading and combine INDRA Statements when done.',
        description='Run reading and combine INDRA Statements when done.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    return parser


if __name__ == '__main__':

    parser = create_parser()
    args = parser.parse_args()

    job_ids = None
    sub = PmidSubmitter(args.basename, args.readers, args.project,
                        group_name=args.group_Name)
    sub.set_options(args.force_read, args.force_fulltext)
    if args.job_type in ['read', 'full']:
        sub.submit_reading(args.input_file, args.start_ix, args.end_ix,
                           args.ids_per_job)
    if args.job_type in ['combine', 'full']:
        sub.submit_combine()
