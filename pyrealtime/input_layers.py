import time

from pyrealtime.layer import ProducerMixin, ThreadLayer


class InputLayer(ProducerMixin, ThreadLayer):
    def __init__(self, frame_generator=None, rate=30, *args, **kwargs):
        super().__init__( *args, **kwargs)
        self._generate = frame_generator if frame_generator is not None else self.generate
        self.rate = rate

    def generate(self, counter):
        return counter

    def get_input(self):
        time.sleep(1.0/self.rate)
        data = self._generate(self.counter)
        self.tick()
        return data

class OneShotInputLayer(ProducerMixin, ThreadLayer):
    def __init__(self, value, *args, **kwargs):
        super().__init__( *args, **kwargs)
        self.value = value

    def generate(self, counter):
        return counter

    def get_input(self):
        if self.counter == 0:
            return self.value
        else:
            time.sleep(1)
            return


# This layer fires exactly 'num_shots' times and can execute 'completion_handler' when the last shot
# is fired.
# num_shots: number of times the layer will fire. num_shots > 1
# completion_handler: function to execute when the last shot is fired. If 'finish' is True,
#                   'completion_handler' will execute when the 'finish' shot is fired.
# finish: Default is False. If finish flag is passed, 1.0/rate seconds after the last shot is fired,
#         the layer will fire a shot with value -1. Also, if 'finish' is True, 'completion_handler'
#         will execute when the 'finish' shot is fired.
# Returns the value returned by the 'frame_generator' function. If finish is passed, after
# the last shot, a -1 shot is fired.

class MultipleShotInputLayer(ProducerMixin, ThreadLayer):
    def __init__(self, num_shots=1, completion_handler=None, frame_generator=None, rate=30, finish=False, *args, **kwargs):
        super().__init__( *args, **kwargs)
        self.rate = rate
        self._generate = frame_generator if frame_generator is not None else self.generate
        self.num_shots = num_shots if finish is False else num_shots + 1
        self.finish = finish
        self.completion_handler = completion_handler if completion_handler is not None else self.default_completion_handler
        self.expired = False

    def generate(self, counter):
        return counter

    def default_completion_handler(self):
        print("Complete!")

    def get_input(self):
        if self.counter < self.num_shots-1:
            time.sleep(1.0/self.rate)
            data = self._generate(self.counter)
            self.tick()
            return data
        elif self.counter == self.num_shots - 1:
            time.sleep(1.0/self.rate)
            self.tick()
            if self.completion_handler is not None:
                self.completion_handler()
            if self.finish:
                return -1
        else:
            if not self.expired:
                self.expired = True
            time.sleep(1.0/self.rate)
            return
