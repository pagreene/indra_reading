import os
import re
import html
import glob
import json
import logging
from pyjnius import autoclass
from os import path, remove, listdir
from indra.config import get_config
from collections import defaultdict
from indra_reading.readers.core import Reader
from indra_reading.readers.util import get_dir


logger = logging.getLogger(__name__)

mti_email = get_config('MTI_EMAIL')
mti_username = get_config('MTI_USERNAME')
mti_password = get_config('MTI_PASSWORD')


def sanitize_text(txt):
    txt = html.unescape(txt)
    txt = re.sub(r'\n', ' ', txt)
    txt = re.sub(r'[^\x00-\x7F]+', ' ', txt)
    return txt


class MTIReader(Reader):
    name = 'MTI'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.num_input = 0
        self.input_dir = get_dir(self.tmp_dir, 'input')
        self.output_dir = get_dir(self.tmp_dir, 'output')

    @classmethod
    def get_version(cls):
        return '1.0'

    def prep_input(self, content_iter):
        logger.info('Prepping input.')
        for content in content_iter:
            # If it's an NXML, we skip it
            if content.is_format('nxml'):
                continue
            quality_issue = self._check_content(content.get_text())
            if quality_issue is not None:
                logger.warning('Skipping %s due to: %s'
                               % (content.get_id(), quality_issue))
                continue

            new_fpath = content.copy_to(self.input_dir)
            self.num_input += 1
            logger.debug('%s saved for tagging by MTI.' % new_fpath)
        return

    def clear_input(self):
        """Remove all the input files (at the end of a reading)."""
        for item in listdir(self.input_dir):
            item_path = path.join(self.input_dir, item)
            if path.isfile(item_path):
                remove(item_path)
                logger.debug('Removed input %s.' % item_path)
        return

    def get_output(self):
        """Get the output of a reading job as a list of filenames."""
        logger.info('Getting MTI outputs.')
        for txt_file in glob.glob(path.join(self.output_dir, '*.txt')):
            content_id = path.splitext(path.basename(txt_file))[0]
            logger.info('Content ID: %s' % content_id)
            with open(txt_file, 'r') as fh:
                content = fh.read()
            self.add_result(content_id, content)
        return self.results

    def _read(self, content_iter, verbose=False, log=False):
        ret = []

        if not self.num_input:
            return ret

        abs_file = os.path.join(self.input_dir, 'abstracts.txt')
        with open(abs_file, 'w') as fh:
            for content in content_iter:
                fh.write('AB - %s\n' % sanitize_text(content.get_text()))

        batch = autoclass('GenericBatchNew')()
        result = batch.processor(["--email", mti_email, abs_file],
                                 mti_username, mti_password)

        result_by_id = defaultdict(list)
        for line in result.splitlines():
            parts = line.split('|')
            content_id = parts[0]
            result_by_id[content_id].append(parts[1:])
        for content_id, res in result_by_id.items():
            out_file = os.path.join(self.output_dir,
                                    '%s.txt' % content_id)
            with open(out_file, 'w') as fh:
                for line in res:
                    fh.write('%s\n' % line)
        # Get the output
        ret = self.get_output()
        self.clear_input()
        return ret

    @staticmethod
    def get_processor(content):
        class DummyProcessor:
            statements = []
        return DummyProcessor()
