from collections import defaultdict

from future.utils import iteritems

import re
from django.core.exceptions import ValidationError


class Transformer(object):
    """Base transformer. Django forms can be used instead.
    This class contains only the bare minimum of methods
    and is able to process a list of forms."""
    forms = []
    error = None
    # dictionary of mappings applied in remap
    mappings = {}
    # dictionary of fieldnames and regexes for invalid values
    blacklist = {}
    defaults = {}

    def __init__(self, dic, defaults=None):
        self.dic = dic
        if defaults:
            self.defaults = defaults
        self.mappings = self._flatten_mappings()

    def _flatten_mappings(self, prefix=None, dic=None):
        def with_prefix(n):
            if prefix:
                return '%s.%s' % (prefix, n)
            return n

        dic = dic or self.mappings
        res = {}
        for k, v in dic.items():
            if isinstance(v, dict):
                res.update(self._flatten_mappings(with_prefix(k), v))
            else:
                res[with_prefix(k)] = v
        return res

    def _process_forms(self, dic):
        """Processes a list of forms."""
        for form in self.forms:
            frm = form(dic)
            if frm.is_valid():
                dic.update(frm.cleaned_data)
            else:
                raise ValidationError(frm.errors)
        return dic

    def _apply_defaults(self, dictionary):
        """Adds defaults to the dictionary."""
        if type(self.defaults) is dict:
            dic = self.defaults.copy()
        else:
            dic = {}
        dictionary.update(dic)
        return dictionary

    def check_blacklist(self, dic):
        """
        Raise ValidationError if value or pattern is
        black-listed.
        """
        for key, value in iteritems(self.blacklist):
            for v in value:
                try:
                    if re.match(v, dic[key]):
                        raise ValidationError(
                            'Value {} not allowed in field {}'.format(
                                v, key))
                except TypeError:
                    raise ValidationError(
                        'Black list test failed, check your blacklist.')

    def validate(self, dic):
        """Raise validation errors here."""
        pass

    def remap(self, dic):
        """Use this method for remapping dictionary keys."""
        data = dic.copy()
        for key in self.mappings:
            m_key = self.mappings[key]
            data[key] = dic[m_key]
            if m_key != key:
                try:
                    del data[m_key]  # delete remapped fields from results
                except KeyError:
                    pass
        return data

    def remap_relations(self, dic):
        def dd():
            return defaultdict(dd)

        data = dd()
        for name, value in dic.items():
            p = data
            parts = name.split('.')
            for n in parts[:-1]:
                p = p[n]
            p[parts[-1]] = value
        return data

    def transform(self, dic):
        """Additional transformations not covered by remap and forms."""
        return dic

    def full_transform(self, dic):
        """Runs all four transformation steps."""
        # Order is important here
        dic = self.remap(dic)
        dic = self._apply_defaults(dic)
        dic = self._process_forms(dic)
        self.check_blacklist(dic)
        dic = self.remap_relations(dic)
        dic = self.transform(dic)
        self.validate(dic)
        return dic

    def clean(self, dic):
        """For compatibility with Django's form class."""
        return self.full_transform(dic)

    def is_valid(self):
        try:
            self.cleaned_data = self.clean(self.dic)
            return True
        except (ValidationError, UnicodeEncodeError) as e:
            self.error = e
            return False
