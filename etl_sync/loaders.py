from __future__ import print_function
from backports import csv
from builtins import str as text

import os
from datetime import datetime
from django.core.exceptions import ValidationError
from django.db import IntegrityError, DatabaseError
from django.conf import settings
from etl_sync.generators import InstanceGenerator
from etl_sync.transformations import Transformer


def get_logfilename(filename):
    ret = None
    if isinstance(filename, (text, str)):
        ret = os.path.join(
            os.path.dirname(filename), '{0}.{1}.log'.format(
            filename, datetime.now().strftime('%Y-%m-%d')))
    return ret


def create_logfile(filename=None):
    if filename:
        return open(filename, 'w')
    else:
        return None


def get_logfile(filename=None, logfilename=None):
    if not logfilename:
        logfilename = get_logfilename(filename)
    return create_logfile(logfilename)


class FeedbackCounter(object):
    """
    Keeps track of the ETL process and provides feedback.
    """

    def __init__(self, message=None, feedbacksize=5000, counter=0):
        self.counter = counter
        self.feedbacksize = feedbacksize
        self.message = message
        self.rejected = 0
        self.created = 0
        self.updated = 0
        self.starttime = datetime.now()
        self.feedbacktime = self.starttime

    def feedback(self):
        """
        Print feedback.
        """
        print(
            '{0} {1} processed in {2}, {3}, {4} created, {5} updated, '
            '{6} rejected'.format(
                self.message, self.feedbacksize,
                datetime.now()-self.feedbacktime, self.counter,
                self.created, self.updated, self.rejected))
        self.feedbacktime = datetime.now()

    def increment(self):
        self.counter += 1

    def reject(self):
        self.rejected += 1
        self.increment()

    def create(self):
        self.created += 1
        self.increment()

    def update(self):
        self.updated += 1
        self.increment()

    def use_result(self, res):
        """
        Use feedback from InstanceGenerator to set counters.
        """
        if res.get('created'):
            self.create()
        elif res.get('updated'):
            self.update()
        else:
            self.increment()

    def finished(self):
        """
        Provides a final message.
        """
        return (
            'Data extraction finished {0}\n\n{1} '
            'created\n{2} updated\n{3} rejected'.format(
                datetime.now(), self.created, self.updated,
                self.rejected))


class Extractor(object):
    """
    Context manager, creates the reader and handles files. This seems
    to be necessary since arguments to CSVDictReader require to be set
    on initialization.

    Return reader instance.
    """
    reader_class = csv.DictReader
    reader_kwargs = {'delimiter': u'\t', 'quoting': csv.QUOTE_NONE}
    encoding = 'utf-8'

    def __init__(self, source, reader_class=None,
                 reader_kwargs = {}, encoding=None):
        self.source = source
        self.reader_class = reader_class or self.reader_class  # deprecate kwarg
        self.reader_kwargs = reader_kwargs or self.reader_kwargs  # deprecate kwarg
        self.encoding = encoding or self.encoding  # deprecate kwarg
        self.fil = None
        self.logfile = None

    def __enter__(self):
        """
        Checks whether source is file object as required by csv.Reader.
        If not, it passes path to reader class. Implement file handling
        in your own reader class. Allows for non-text data sources or
        directories (see e.g. OGRReader)
        """
        if hasattr(self.source, 'read'):
            fil = self.source
        else:
            try:
                fil = open(self.source)
            except IOError:
                return None
        return self.reader_class(fil, **self.reader_kwargs)

    def __exit__(self, type, value, traceback):
        try:
            self.fil.close()
        except (AttributeError, IOError):
            pass


class Logger(object):
    """Class that holds the logger messages."""
    start_message = (
        'Data extraction started {start_time}\n\nStart line: '
        '{slice_begin}\nEnd line: {slice_end}\n')
    reader_error_meassage = (
        'Text decoding or CSV error in line {0}: {1} => rejected')
    instance_error_message = (
        'Instance generation error in line {0}: {1} => rejected')
    instance_error_message = (
        'Transformation error in line {0}: {1} => rejected')

    def __init__(self, logfile):
        self.logfile = logfile

    def log(self, txt):
        """
        Log to log file or to stdout if self.logfile=None
        """
        print(text(txt), file=self.logfile)

    def log_start(self, options):
        self.log(self.start_message.format(**options))

    def log_reader_error(self, line, error):
        self.log(self.reader_error_message.format(line, str(error)))

    def log_transformation_error(self, line, error):
        self.log(self.transformation_error_message.format(line, str(error)))

    def log_instance_error(self, line, error):
        self.log(self.instance_error_message.format(line, str(error)))

    def close(self):
        if self.logfile:
            self.logfile.close()


class Loader(object):
    """
    Generic mapper object for ETL.
    """
    transformer_class = Transformer
    extractor_class = Extractor
    generator_class = InstanceGenerator
    model_class = None
    filename = None # move to init
    encoding = 'utf-8' # to be deprecated
    slice_begin = 0 # move to init
    slice_end = None # move to init
    defaults = {} # to be deprecated in 1.0, set in Transformer class
    create_new = True
    update = True
    create_foreign_key = True
    etl_persistence = ['record']
    message = 'Data Extraction'
    result = None
    logfilename = None

    def __init__(self, *args, **kwargs):
        self.filename = kwargs.get('filename')
        self.model_class = kwargs.get('model_class') or self.model_class
        self.feedbacksize = getattr(settings, 'ETL_FEEDBACK', 5000)
        self.logfile = get_logfile(
            filename=self.filename, logfilename=self.logfilename)
        self.extractor = self.extractor_class(self.filename)

    def feedback_hook(self, counter):
        """Create actions that will be triggered after the number of records
        defined in self.feedbacksize. This can be used to store a file position
        to a database to continue a load later.
        """
        pass

    def load(self):
        """
        Loads data into database using Django models and error logging.
        """
        print('Opening {0} using {1}'.format(self.filename, self.encoding))
        logger = Logger(self.logfile)
        counter = FeedbackCounter(
            feedbacksize=self.feedbacksize, message=self.message)
        with self.extractor as extractor:
            logger.log_start({
                'start_time': datetime.now().strftime('%Y-%m-%d'),
                'slice_begin': self.slice_begin,
                'slice_end': self.slice_end})
            while self.slice_begin and self.slice_begin > counter.counter:
                extractor.next()
                counter.increment()
            while not self.slice_end or self.slice_end >= counter.counter:
                try:
                    csv_dic = extractor.next()
                except (UnicodeDecodeError, csv.Error) as e:
                    logger.log_reader_error(counter.counter, e)
                    counter.reject()
                    continue
                except StopIteration:
                    logger.log('End of file.')
                    break
                transformer = self.transformer_class(csv_dic, self.defaults)
                if transformer.is_valid():
                    dic = transformer.cleaned_data
                else:
                    logger.log_transformation_error(
                        counter.counter, transformer.error)
                    counter.reject()
                    continue
                generator = self.generator_class(
                    self.model_class, dic,
                    persistence=self.etl_persistence)
                try:
                    generator.get_instance()
                except (ValidationError, IntegrityError, DatabaseError) as e:
                    logger.log_instance_error(counter.counter, e)
                    counter.reject()
                    continue
                else:
                    counter.use_result(generator.res)
                if counter.counter % self.feedbacksize == 0:
                    counter.feedback()
                    self.feedback_hook(counter.counter)
            logger.log(counter.finished())
            logger.close()
        return 'finished'
