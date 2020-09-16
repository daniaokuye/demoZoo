# encoding: utf-8
'''
@author: **
@license: Apache Licence
@software: PyCharm
@file: utitlies.py
@time: 2020/8/25 10:21 AM
'''

from __future__ import print_function
import multiprocessing
import itertools
import os
import sys
import traceback
import threading
from functools import wraps
import time
import gc

try:
    if sys.version > '3':
        import queue
        from queue import Empty
    else:
        import Queue as queue
        from Queue import Empty
except AttributeError:
    class Empty(Exception):
        'Exception raised by Queue.get(block=0)/get_nowait().'
        pass
MP_STATUS_CHECK_INTERVAL = 1.0

__all__ = ['dataloader', 'dataloader2', 'thread_process', 'thread_process2', 'ExceptionWrapper',
           'Empty', 'simple_threading', 'simple_threading_parameters']


class KeyErrorMessage(str):
    r"""str subclass that returns itself in repr"""

    def __repr__(self):
        return self


class ExceptionWrapper(object):
    r"""Wraps an exception plus traceback to communicate across threads"""

    def __init__(self, exc_info=None, where="in background"):
        # It is important that we don't store exc_info, see
        # NOTE [ Python Traceback Reference Cycle Problem ]
        if exc_info is None:
            exc_info = sys.exc_info()
        self.exc_type = exc_info[0]
        self.exc_msg = "".join(traceback.format_exception(*exc_info))
        self.where = where

    def reraise(self):
        r"""Reraises the wrapped exception in the current thread"""
        # Format a message such as: "Caught ValueError in DataLoader worker
        # process 2. Original Traceback:", followed by the traceback.
        msg = "Caught {} {}.\nOriginal {}".format(
            self.exc_type.__name__, self.where, self.exc_msg)
        if self.exc_type == KeyError:
            # KeyError calls repr() on its argument (usually a dict key). This
            # makes stack traces unreadable. It will not be changed in Python
            # (https://bugs.python.org/issue2651), so we work around it.
            msg = KeyErrorMessage(msg)
        raise self.exc_type(msg)

    def __repr__(self):
        msg = "Caught {} {}.\nOriginal {}".format(
            self.exc_type.__name__, self.where, self.exc_msg)
        return msg


def clear(q):
    # How to clear a multiprocessing queue in python
    # https://stackoverflow.com/questions/16461459/how-to-clear-a-multiprocessing-queue-in-python
    try:
        while True:
            q.get_nowait()
    except Empty:
        pass


class ManagerWatchdog(object):
    def __init__(self):
        self.manager_pid = os.getppid()
        self.manager_dead = False

    def is_alive(self):
        if not self.manager_dead:
            self.manager_dead = os.getppid() != self.manager_pid
        return not self.manager_dead


def simple_threading(functions, parameters):
    ret_flag = []

    def trace_func(func, index, *args, **kwargs):
        """
        @note:替代profile_func，新的跟踪线程返回值的函数，对真正执行的线程函数包一次函数，以获取返回值
        """
        ret = func(*args, **kwargs)
        ret_flag.append((index, ret))

    threads = []
    for index in range(len(functions)):
        pms = [functions[index], index] + list(parameters)
        one_thread = threading.Thread(target=trace_func, args=tuple(pms))
        one_thread.start()
        threads.append(one_thread)
    for t in threads:
        t.join()
    thread_result = dict(ret_flag)
    del threads
    gc.collect()
    return thread_result


def simple_threading_parameters(function, multi_parameters):
    ret_flag = []

    def trace_func(func, index, *args, **kwargs):
        """
        @note:替代profile_func，新的跟踪线程返回值的函数，对真正执行的线程函数包一次函数，以获取返回值
        """
        ret = func(*args, **kwargs)
        ret_flag.append((index, ret))

    threads = []
    for index in range(len(multi_parameters)):
        pms = [function, index] + list(multi_parameters[index])
        one_thread = threading.Thread(target=trace_func, args=tuple(pms))
        one_thread.start()
        threads.append(one_thread)
    for t in threads:
        t.join()
    thread_result = dict(ret_flag)
    return thread_result


# multi process
class dataloader(object):
    def __init__(self, collate_fn, sub_collate=None, num_procs_workers=4):
        self._num_procs_workers = num_procs_workers
        self._worker_result_queue = multiprocessing.Queue()
        self._procs_done_event = multiprocessing.Event()
        self.condition = multiprocessing.Condition()
        self._procs_worker_queue_idx_cycle = itertools.cycle(range(self._num_procs_workers))
        self._procs_workers_status = []
        self._procs_queues = []
        self._procs_workers = []
        self.init(collate_fn, sub_collate)

    def init(self, collate_fn, sub_collate=None):
        def local_fun(fun, subfun):
            @wraps(fun)
            def wrapper(*args):
                print(args)
                subres = subfun.bag(*args)
                res = fun(*args)
                try:
                    if res[0] == 'Batch':
                        res[3][-6] = subres  # kandianFilter
                    else:
                        res[2]['kandianFilter'] = subres
                except:
                    res = [subres, res]
                return res

            return wrapper

        for i in range(self._num_procs_workers):
            proc_queue = multiprocessing.Queue()
            new_collate_fn = collate_fn if sub_collate is None else local_fun(collate_fn, sub_collate())
            w = multiprocessing.Process(
                target=procs_worker_loop,
                args=(new_collate_fn, proc_queue, self._worker_result_queue, self._procs_done_event, i,
                      self._num_procs_workers, self.condition))
            w.daemon = True
            w.start()
            self._procs_queues.append(proc_queue)
            self._procs_workers.append(w)
            self._procs_workers_status.append(True)

    def preload_do(self, urls, coverurl, req_id, script):
        clear(self._worker_result_queue)
        for idx_batch, eachUrl in enumerate(urls):
            sole_args = (eachUrl, coverurl, req_id, script)
            worker_queue_idx = next(self._procs_worker_queue_idx_cycle)
            if not self._procs_workers_status[worker_queue_idx]:
                continue
            self._procs_queues[worker_queue_idx].put(sole_args)

    def _shutdown_worker(self, worker_id):
        assert self._procs_workers_status[worker_id]
        # Signal termination to that specific worker.
        q = self._procs_queues[worker_id]
        # Indicate that no more data will be put on this queue by the current
        # process.
        q.put(None)

        # Note that we don't actually join the worker here, nor do we remove the
        # worker's pid from C side struct because (1) joining may be slow, and
        # (2) since we don't join, the worker may still raise error, and we
        # prefer capturing those, rather than ignoring them, even though they
        # are raised after the worker has finished its job.
        # Joinning is deferred to `_shutdown_workers`, which it is called when
        # all workers finish their jobs (e.g., `IterableDataset` replicas) or
        # when this iterator is garbage collected.
        self._procs_workers_status[worker_id] = False

    def _shutdown_workers(self):
        # Called when shutting down this `_MultiProcessingDataLoaderIter`.
        # See NOTE [ Data Loader Multiprocessing Shutdown Logic ] for details on
        # the logic of this function.
        # python_exit_status = _utils.python_exit_status
        # if python_exit_status is True or python_exit_status is None:
        #     # See (2) of the note. If Python is shutting down, do no-op.
        #     return
        # Normal exit when last reference is gone / iterator is depleted.
        # See (1) and the second half of the note.
        try:
            # Exit workers now.
            self._procs_done_event.set()
            for worker_id in range(len(self._procs_workers)):
                # Get number of workers from `len(self._workers)` instead of
                # `self._num_workers` in case we error before starting all
                # workers.
                if self._procs_workers_status[worker_id]:
                    self._shutdown_worker(worker_id)
            for w in self._procs_workers:
                w.join()
            for q in self._procs_queues:
                q.cancel_join_thread()
                q.close()
        finally:
            # Even though all this function does is putting into queues that
            # we have called `cancel_join_thread` on, weird things can
            # happen when a worker is killed by a signal, e.g., hanging in
            # `Event.set()`.
            pass

    def __del__(self):
        self._shutdown_workers()


class dataloader2(dataloader):
    def __init__(self, collate_cls, sub_collate=None, num_procs_workers=4):
        # collate_cls is an class
        super(dataloader2, self).__init__(collate_cls, None, num_procs_workers)

    def init(self, collate_cls, sub_collate=None):
        for i in range(self._num_procs_workers):
            proc_queue = multiprocessing.Queue()
            new_collate_fn = collate_cls()
            w = multiprocessing.Process(
                target=procs_worker_loop,
                args=(new_collate_fn, proc_queue, self._worker_result_queue, self._procs_done_event, i,
                      self._num_procs_workers, self.condition))
            w.daemon = True
            w.start()
            self._procs_queues.append(proc_queue)
            self._procs_workers.append(w)
            self._procs_workers_status.append(True)

    def preload_do(self, urls, coverurl, req_id, script):
        clear(self._worker_result_queue)
        super(dataloader2, self).preload_do(urls, coverurl, req_id, script)

        # collect data
        outputs = []
        with self.condition:
            WaitTime = 2
            for _ in urls:
                self.condition.wait(timeout=WaitTime)
                try:
                    output = self._worker_result_queue.get(timeout=WaitTime)
                    outputs.append(output)
                except Empty:
                    break
                except:
                    print(traceback.format_exc())
                WaitTime = 0.5
        return outputs


def procs_worker_loop(collate_fn, proc_queue, _worker_result_queue, done_event, worker_id,
                      num_workers, condition):
    try:
        watchdog = ManagerWatchdog()

        while watchdog.is_alive():
            try:
                r = proc_queue.get(timeout=MP_STATUS_CHECK_INTERVAL)
            except Empty:
                continue
            if r is None:
                # Received the final signal
                assert done_event.is_set()
                break
            elif done_event.is_set():
                # `done_event` is set. But I haven't received the final signal
                # (None) yet. I will keep continuing until get it, and skip the
                # processing steps.
                continue
            # Info_, tfimgClassRes_bt, idx_batch, eachImg, single_image, req_id, script = r
            try:
                if len(r) == 4:
                    time.sleep(0.005 * worker_id)
                    output = collate_fn(*r)
                else:
                    r = list(r)
                    horse = r[0]
                    output = collate_fn(*r[1:])
                    horse.set_result(output)

            except Exception as e:
                output = ExceptionWrapper(where="in DataLoader worker process {}/{}".format(worker_id, num_workers))
            if len(r) == 4:
                _worker_result_queue.put(output)
                with condition:
                    condition.notify_all()
            del r, output
    except KeyboardInterrupt:
        # Main process will raise KeyboardInterrupt anyways.
        pass
    if done_event.is_set():
        _worker_result_queue.cancel_join_thread()
        _worker_result_queue.close()


class ActorExit(Exception):
    pass


class thread_process(object):
    def __init__(self, collate_fns):
        self._thread_done_events = []
        self._collect_queue = multiprocessing.Queue()
        self.condition = multiprocessing.Condition()
        self._procs_queues = []
        self._procs_workers = []
        self.init(collate_fns)

    def init(self, collate_fns):
        self.num_procs_workers = len(collate_fns)
        for i in range(self.num_procs_workers):
            local_queue = multiprocessing.Queue()
            ET = threading.Event()
            w = threading.Thread(
                target=module_loop,
                args=(collate_fns[i], local_queue, self._collect_queue,
                      i, ET, self.condition)
            )
            w.daemon = True
            w.start()
            self._procs_queues.append(local_queue)
            self._procs_workers.append(w)
            self._thread_done_events.append(ET)

    def __call__(self, input):
        clear(self._collect_queue)
        bad_thread = set()
        for i in range(self.num_procs_workers):
            try:
                self._procs_queues[i].put(input, timeout=MP_STATUS_CHECK_INTERVAL)
            except:
                bad_thread.add(i)
                continue
        toload = set(range(self.num_procs_workers)) - bad_thread
        return self.collect_data(toload)

    def collect_data(self, no_procs_workers):
        outs = []
        with self.condition:
            WaitTime = 2
            for i in no_procs_workers:
                self.condition.wait(timeout=WaitTime)
                try:
                    res = self._collect_queue.get(timeout=WaitTime)
                    # prepare newImgInfo
                    outs.append(res)
                except Empty:
                    break
                WaitTime = 0.5
        newImgInfo = dict(outs)
        return newImgInfo

    def __del__(self):
        clear(self._collect_queue)
        for i in range(self.num_procs_workers):
            self._procs_queues[i].put(ActorExit)
            self._thread_done_events[i].wait()
        self.collect_data(range(self.num_procs_workers))
        # del task or task done
        self._collect_queue.cancel_join_thread()
        self._collect_queue.close()


class thread_process2(thread_process):
    def __call__(self, inputs):
        nums = len(inputs)
        clear(self._collect_queue)
        bad_thread = set()
        for i in range(nums):
            try:
                self._procs_queues[i].put(inputs[i], timeout=MP_STATUS_CHECK_INTERVAL)
            except:
                bad_thread.add(i)
                continue
        toload = set(range(nums)) - bad_thread
        return self.collect_data(toload)


def module_loop(collate_fn, in_queue, out_queue, module_id, done_event, condition):
    while True:
        try:
            data = in_queue.get(timeout=MP_STATUS_CHECK_INTERVAL)
        except Empty:
            continue
        if data is ActorExit:
            done_event.set()
            break
        if not isinstance(data, ExceptionWrapper):
            try:
                data = collate_fn(*data)
            except Exception:
                data = ExceptionWrapper(
                    where="in pin memory thread for index {}".format(module_id))
        r = (module_id, data)
        out_queue.put(r, timeout=MP_STATUS_CHECK_INTERVAL)
        with condition:
            condition.notify_all()
        del r, data
