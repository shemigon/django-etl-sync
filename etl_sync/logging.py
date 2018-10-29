from __future__ import print_function

from datetime import datetime


class Counter(object):
    def __init__(self):
        self.pos = 1
        self.rejected = 0
        self.created = 0
        self.updated = 0
        self.start_time = datetime.now()
        self.finish_time = None

    def next(self):
        self.pos += 1

    def finish(self):
        self.finish_time = datetime.now()

    def create(self):
        self.created += 1
        self.next()

    def update(self):
        self.created += 1
        self.next()

    def reject(self):
        self.rejected += 1
        self.next()


class BaseLogger(object):
    counter_class = Counter
    filename = None

    def __init__(self, counter_class=None):
        self.counter = None
        if counter_class is not None:
            self.counter_class = counter_class

    def status(self, msg, *args):
        pass

    def start(self):
        self.counter = self.counter_class()

    def finish(self):
        self.counter.finish()

    def accept(self, action, dic, instance):
        if action == 'created':
            self.counter.create()
        elif action == 'updated':
            self.counter.update()
        else:
            self.counter.next()

    def reject(self, msg, dic):
        self.counter.reject()

    def skip(self):
        self.counter.next()


class StdoutLogger(BaseLogger):
    def status(self, msg, *args):
        print(msg % args)

    def reject(self, msg, dic=None):
        super(StdoutLogger, self).reject(msg, dic)
        print('Error, row {}: {}'.format(self.counter.pos, msg))

    def finish(self):
        super(StdoutLogger, self).finish()
        lines = [
            '',
            'Data extraction started {}'.format(self.counter.start_time),
            '',
            '{} created'.format(self.counter.created),
            '{} updated'.format(self.counter.updated),
            '{} rejected'.format(self.counter.rejected),
            '',
            'Data extraction finished {}'.format(self.counter.finish_time),
            'Time spent: {}'.format(self.counter.finish_time -
                                    self.counter.start_time),
            '',
        ]
        print('\n'.join(lines))
