import queue
import threading
import multiprocessing
from datetime import datetime, timedelta
from time import sleep
from enum import Enum

from pyrealtime.layer_manager import LayerManager


class LayerTrigger(Enum):
    SLOWEST = 0
    FASTEST = 1
    LAYER = 2
    TIMER = 3


class LayerSignal(Enum):
    STOP = 0


class BasePort(object):
    def get_output(self):
        raise NotImplementedError

    def handle_output(self, data):
        raise NotImplementedError


class Port(BasePort):
    def __init__(self):
        self.out_queues = []

    def get_output(self):
        ctx = multiprocessing.get_context('spawn')
        out_queue = ctx.Queue()
        self.out_queues.append(out_queue)
        return out_queue

    def handle_output(self, data):
        if data is not None:
            for queue in self.out_queues:
                queue.put(data)


class BaseOutputLayer(BasePort):
    def __init__(self, *args, **kwargs):
        self.out_port = Port()

    def handle_output(self, data):
        self.out_port.handle_output(data)

    def get_output(self):
        return self.out_port.get_output()


class BaseInputLayer(object):

    def get_input(self):
        raise NotImplementedError


class BaseLayer(BaseInputLayer, BaseOutputLayer):

    def __init__(self, signal_in=None, name="", time_window=timedelta(seconds=5), print_fps=False, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.name = name
        self.counter = 0
        self.signal = None
        self.is_first = True
        self.stop_event = None
        self.signal_in = None
        self.set_signal_in(signal_in)

        self.count = 0
        self.start_time = None
        self.reset()
        self.time_window = time_window
        self.print_fps = print_fps
        self.fps = 0

    def tick(self):
        t = datetime.now()
        self.count += 1
        if t - self.start_time >= self.time_window:
            self.fps = self.count / (t - self.start_time).total_seconds()
            if self.print_fps:
                print(self.fps)
            self.reset()

    def reset(self):
        self.count = 0
        self.start_time = datetime.now()

    def post_init(self, data):
        pass

    def start(self, stop_event):
        self.stop_event = stop_event

    def stop(self):
        self.stop_event.set()

    def transform(self, data):
        return data

    def initialize(self):
        pass

    def set_signal_in(self, signal_in):
        self.signal_in = signal_in.get_output() if signal_in is not None else None

    def get_signal(self):
        self.signal = None
        if self.signal_in is not None:
            while not self.signal_in.empty():
                self.signal = self.signal_in.get()
                self.handle_signal(self.signal)

    def handle_signal(self, signal):
        pass

    def process_loop(self):
        while not self.stop_event.is_set():
            data = self.get_input()
            if isinstance(data, LayerSignal) and data == LayerSignal.STOP:
                self.stop()
                continue

            if data is None:
                continue

            self.get_signal()
            if self.is_first:
                self.post_init(data)
                self.is_first = False
            data_transformed = self.transform(data)
            if data_transformed is None:
                continue
            self.handle_output(data_transformed)
            self.tick()
            if isinstance(data, LayerSignal) and data_transformed == LayerSignal.STOP:
                self.stop()
            self.counter += 1
        self.handle_output(LayerSignal.STOP)
        self.shutdown()

    def shutdown(self):
        pass

    def join(self):
        raise NotImplementedError


class ThreadLayer(BaseLayer):
    def __init__(self, parent_proc=None, *args, **kwargs):
        # print("thread layer init")
        super().__init__(*args, **kwargs)
        if parent_proc is not None:
            self.thread = parent_proc.register_child_thread(self)
        else:
            self.create_thread()
            LayerManager.add_layer(self)

    def create_thread(self):
        self.thread = threading.Thread(target=self.run_thread)
        self.thread.daemon = True

    def run_thread(self):
        self.initialize()
        self.process_loop()

    def start(self, *args, **kwargs):
        super(ThreadLayer, self).start(*args, **kwargs)
        self.thread.start()

    def join(self):
        self.thread.join()


class ProcessLayer(BaseLayer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        ctx = multiprocessing.get_context('spawn')
        self.process = ctx.Process(target=self.run_proc)
        self.thread_layers = []
        LayerManager.add_layer(self)

    def run_proc(self):
        self.init_child_threads()
        self.initialize()
        t = threading.Thread(target=self.process_loop)
        t.daemon = False
        t.start()

        for thread_layer in self.thread_layers:
            thread_layer.create_thread()
            thread_layer.start(stop_event=self.stop_event)

        self.main_thread_post_init()

    def main_thread_post_init(self):
        pass

    def start(self, *args, **kwargs):
        super(ProcessLayer, self).start(*args, **kwargs)
        self.process.start()

    def join(self):
        self.process.join()

    def init_child_threads(self):
        for thread_layer in self.thread_layers:
            thread_layer.create_thread()

    def register_child_thread(self, thread_layer):
        self.thread_layers.append(thread_layer)


class MultiOutputMixin(BaseOutputLayer):
    def __init__(self, *args, **kwargs):
        self.ports = {}
        self.auto_ports = {}
        super().__init__(*args, **kwargs)

    def get_port(self, port):
        if port in self.ports:
            return self.ports[port]
        if port in self.auto_ports:
            return self.auto_ports[port]
        self._register_port(port, auto=True)
        if port in self.auto_ports:
            return self.auto_ports[port]
        raise NameError("Port %s does not exist" % port)

    def _register_port(self, port, auto=False):
        port_list = self.ports if auto is False else self.auto_ports
        if port in port_list:
            raise NameError("Port %s already exists" % port)
        port_list[port] = Port()

    def handle_output(self, data):
        if data is not None:
            for key in list(self.ports.keys()) + list(self.auto_ports.keys()):
                if key in self.ports:
                    port = self.ports[key]
                elif key in self.auto_ports:
                    port = self.auto_ports[key]
                else:
                    raise NameError("Port %s does not exist" % key)
                try:
                    port.handle_output(data[key])
                except KeyError:
                    pass
        super().handle_output(data)



class ProducerMixin(BaseInputLayer):

    def get_input(self):
        raise NotImplementedError


class TransformMixin(BaseInputLayer):

    def __init__(self, port_in, trigger=LayerTrigger.SLOWEST, trigger_source=None, discard_old=False, *args, **kwargs):
        self.ports_in = {}
        self.keys = []
        self.discard_old = discard_old
        if port_in is not None:
            self.set_input(port_in)
        self.trigger = trigger
        self.trigger_source = trigger_source
        super().__init__(*args, **kwargs)

    def set_input(self, port_in, key='default'):
        assert(isinstance(port_in, BasePort))
        assert(key not in self.keys)
        self.keys.append(key)
        self.ports_in[key] = port_in.get_output()

    def get_input(self):
        data = None
        if self.trigger == LayerTrigger.TIMER:
            sleep(self.trigger_source)
            data = self.get_all_nowait(self.discard_old)
        elif self.trigger == LayerTrigger.SLOWEST:
            data = self.get_all(self.discard_old)
        elif self.trigger == LayerTrigger.FASTEST:
            data = self.get_any()
        elif self.trigger == LayerTrigger.LAYER:
            data = self.get_ensure_layer(self.trigger_source, self.discard_old)
        else:
            assert(False)

        if self.keys[0] == 'default' and len(self.keys) == 1:
            return data['default']
        return data

    def get_all(self, discard_old):
        data = {}
        for key in self.keys:
            data[key] = self.ports_in[key].get()
            if discard_old:
                try:
                    while True:
                        data[key] = self.ports_in[key].get_nowait()
                except queue.Empty:
                    pass
        return data

    def get_all_nowait(self, discard_old):
        data = {}
        for key in self.keys:
            try:
                while True:
                    data[key] = self.ports_in[key].get_nowait()
                    if not discard_old:
                        break
            except queue.Empty:
                pass
        return data

    def get_any(self):
        value = None
        data = {}
        sleep_time = 0.001
        while True:
            for key in self.keys:
                try:
                    value = self.ports_in[key].get_nowait()
                except queue.Empty:
                    pass
                if value is not None:
                    data[key] = value
                    return data
            sleep(sleep_time)
            sleep_time *= 2

    def get_ensure_layer(self, layer, discard_old):
        data = {}
        assert(layer in self.keys)
        data[layer] = self.ports_in[layer].get()

        for key in self.keys:
            if key == layer:
                continue
            try:
                while True:
                    data[key] = self.ports_in[key].get_nowait()
                    if not discard_old:
                        break
            except queue.Empty:
                pass
        return data


class MergeLayer(TransformMixin, ThreadLayer):
    pass


class TransformLayer(TransformMixin, ThreadLayer):
    def __init__(self, port_in, transformer, *args, **kwargs):
        self.transform = transformer
        super().__init__(port_in, *args, **kwargs)