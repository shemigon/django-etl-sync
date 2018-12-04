from enum import Enum, unique


@unique
class GenerationStatus(Enum):
    Updated = 'updated'
    Exists = 'exists'
    Created = 'created'


class CaseInsensitiveDict(dict):
    def __init__(self, dic=None, **kwargs):
        if dic:
            dic = dict((k.lower(), v) for k, v in dic.items())
        super(CaseInsensitiveDict, self).__init__(dic)

    def __contains__(self, key):
        return super(CaseInsensitiveDict, self).__contains__(key.lower())

    def __getitem__(self, key):
        return super(CaseInsensitiveDict, self).__getitem__(key.lower())

    def __setitem__(self, key, value):
        return super(CaseInsensitiveDict, self).__setitem__(key.lower(), value)

    def __delitem__(self, key):
        return super(CaseInsensitiveDict, self).__delitem__(key.lower())

    def get(self, key, default=None):
        return super(CaseInsensitiveDict, self).get(key.lower(), default)

    def pop(self, key, default=None):
        return super(CaseInsensitiveDict, self).pop(key.lower(), default)

    def setdefault(self, key, default=None):
        return super(CaseInsensitiveDict, self).setdefault(key.lower(), default)

    def update(self, dic, **kwargs):
        d = dict((k.lower(), v) for k, v in dic.items())
        d.update(dict((k.lower(), v) for k, v in kwargs.items()))
        return super(CaseInsensitiveDict, self).update(d)


