from __future__ import absolute_import, print_function

import io

from backports import csv
from django.core.exceptions import ValidationError
from django.db import DatabaseError, IntegrityError, transaction

from .generators import InstanceGenerator
from .logging import StdoutLogger
from .transformations import Transformer
from .types import CaseInsensitiveDict


class Extractor(object):
    """
    Context manager, creates the reader and handles files or other
    sources. This is necessary because parameters to CSVDictReader
    needs to be set on initialization.

    Args:
        source (file, file-like object, or str)
        reader_class (CSVReader or duck-typed Reader class)
        reader_kwargs (dic): Whatever needs to be passed on to the reaader
        options (dic): custom options that need to be passed through to
            reader

    Return reader instance.
    """

    def __init__(self, source, reader_class=None,
                 reader_kwargs=None, options=None):
        self.source = source
        self.options = options or {}
        self.reader_class = reader_class or csv.DictReader
        self.reader_kwargs = reader_kwargs or {
            'delimiter': u'\t',
            'quoting': csv.QUOTE_NONE
        }
        self.fil = None

    def __enter__(self):
        """
        Checks whether source is file object as required by csv.Reader.
        Implement file handling in reader class. This approach allows for
        non-text data sources (see e.g. OGRReader). If self.source is not
        a file or file-like object and cannot be converted in one by opening,
        we pass self.source on. In that case the reader_class needs to make
        sense of it. This is necessary, i.e. if the source is a .gdb
        represented as a folder or an url representing an API end point.
        """
        if hasattr(self.source, 'read'):
            self.fil = self.source
        else:
            try:
                self.fil = io.open(self.source)
            except IOError:
                self.fil = self.source
        return self.reader_class(self.fil, **self.reader_kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.fil.close()
        except (AttributeError, IOError):
            pass


class Logger(object):
    """Class that holds the logger messages."""
    start_message = (
        'Data extraction started {start_time}\n\nStart line: '
        '{slice_begin}\nEnd line: {slice_end}\n')
    reader_error_message = (
        'Text decoding or CSV error in line {0}: {1} => rejected')
    instance_error_message = (
        'Instance generation error in line {0}: {1} => rejected')
    transformation_error_message = (
        'Transformation error in line {0}: {1} => rejected')

    def __init__(self, logfile):
        self.logfile = logfile

    def log(self, txt):
        """
        Log to log file or to stdout if self.logfile=None
        """
        print(str(txt), file=self.logfile)

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
    reader_class = csv.DictReader
    reader_kwargs = None
    generator_class = InstanceGenerator
    model_class = None
    extractor_class = Extractor
    persistence = None

    def __init__(self, source, model_class=None, logger=None, options=None):
        self.source = source
        self.filename = source.name if hasattr(source, 'name') else source
        self.options = options or {}
        self.model_class = model_class or self.model_class
        self.logger = logger or StdoutLogger()
        self.logger.filename = self.filename
        self.extractor = self.extractor_class(self.source, self.reader_class,
                                              self.reader_kwargs,
                                              options=self.options)
        self.slice_begin = self.options.get('slice_begin')
        self.slice_end = self.options.get('slice_end')
        self.generator = self.generator_class(self.model_class,
                                              persistence=self.persistence,
                                              options=self.options)

    def process(self, extractor):
        """
        This is broken out from below and should be better
        organized.
        """

        try:
            dic = extractor.next()
            # dic = CaseInsensitiveDict(extractor.next())
        except (UnicodeDecodeError, csv.Error) as e:
            self.logger.reject(str(e))
            return

        defaults = self.options.get('defaults') or {}
        transformer = self.transformer_class(dic, defaults=defaults)
        try:
            if transformer.is_valid():
                dic = transformer.cleaned_data
            else:
                raise ValidationError(transformer.error)
        except (ValidationError, ValueError, IndexError, KeyError) as e:
            self.logger.reject(str(e), dic)
            return

        try:
            with transaction.atomic():
                instance = self.generator.get_instance(dic)
        except (ValidationError, IntegrityError,
                DatabaseError, ValueError) as exc:
            if hasattr(exc, 'message_dict'):
                msg = ', '.join(' '.join([f, '(%s)' % ', '.join(err)])
                                for f, err in exc.message_dict.items())
            else:
                msg = str(exc)
            self.logger.reject(msg, dic)
            return

        self.logger.accept(self.generator.res, dic, instance)

    def load(self):
        """
        Loads data into database using Django models and error logging.
        """
        self.logger.status('Opening %s.', self.filename)
        self.logger.start()

        with self.extractor as extractor:

            while self.slice_begin and self.slice_begin > self.logger.counter:
                extractor.next()
                self.logger.skip()

            while not self.slice_end or self.slice_end >= self.logger.counter:
                try:
                    self.process(extractor)
                except StopIteration:
                    break

            if self.generator.finalize():
                self.logger.finish()
                return self.logger.counter
