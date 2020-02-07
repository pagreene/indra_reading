import os
from setuptools import setup


readme_path = os.path.join(os.path.abspath(os.path.dirname(__file__)),
                           'README.md')
with open(readme_path, 'r', encoding='utf-8') as fh:
    long_description = fh.read()


def main():
    install_list = ['indra', 'boto3']

    extras_require = {
                      'trips': ['pykqml'],
                      'reach': ['cython', 'pyjnius==1.1.4'],
                      'adeft': ['adeft'],
                      'isi': ['nltk', 'unidecode'],
                      }
    extras_require['all'] = list({dep for deps in extras_require.values()
                                  for dep in deps})

    setup(name='indra_reading',
          version='0.0.1',
          description='High Through-put Reading Tools for INDRA',
          long_description=long_description,
          long_description_content_type='text/markdown',
          author='Patrick Greene',
          author_email='patrick.anton.greene@gmail.com',
          url='http://github.com/indralab/indra_reading',
          packages=['indra_reading', 'indra_reading.pipelines',
                    'indra_reading.pipelines.pmid_reading',
                    'indra_reading.pipelines.starcluster_reading',
                    'indra_reading.readers', 'indra_reading.readers.isi',
                    'indra_reading.readers.reach',
                    'indra_reading.readers.sparser',
                    'indra_reading.readers.trips', 'indra_reading.scripts',
                    'indra_reading.util', 'indra_reading.tests'],
          install_requires=install_list,
          extras_require=extras_require,
          include_package_data=True,
          keywords=['systems', 'biology', 'reading', 'aws',
                    'nlp', 'mechanism', 'biochemistry', 'network'])


if __name__ == '__main__':
    main()
