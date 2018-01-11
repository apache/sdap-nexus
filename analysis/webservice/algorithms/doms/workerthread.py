
import threading


class WorkerThread(threading.Thread):

    def __init__(self, method, params):
        threading.Thread.__init__(self)
        self.method = method
        self.params = params
        self.completed = False
        self.results = None

    def run(self):
        self.results = self.method(*self.params)
        self.completed = True


def __areAllComplete(threads):
    for thread in threads:
        if not thread.completed:
            return False

    return True


def wait(threads, startFirst=False, poll=0.5):
    if startFirst:
        for thread in threads:
            thread.start()

    while not __areAllComplete(threads):
        threading._sleep(poll)


def foo(param1, param2):
    print param1, param2
    return "c"


if __name__ == "__main__":

    thread = WorkerThread(foo, params=("a", "b"))
    thread.start()
    while not thread.completed:
        threading._sleep(0.5)
    print thread.results
