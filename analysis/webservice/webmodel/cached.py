import hashlib
import inspect
import time

__CACHE = {}

def cached(ttl=60000):
    def _hash_function_signature(func):
        hash_object = hashlib.md5(str(inspect.getargspec(func)) + str(func))
        return hash_object.hexdigest()

    def _now():
        return int(round(time.time() * 1000))

    def _expired(t):
        if t is None or _now() - t > ttl:
            return True
        else:
            return False

    def _cached_decorator(func):

        def func_wrapper(self, computeOptions, **args):
            hash = _hash_function_signature(func)
            force = computeOptions.get_boolean_arg("nocached", default=False)

            if force or hash not in __CACHE or (hash in __CACHE and _expired(__CACHE[hash]["time"])):
                result = func(self, computeOptions, **args)
                __CACHE[hash] = {
                    "time": _now(),
                    "result": result
                }

            return __CACHE[hash]["result"]

        return func_wrapper

    return _cached_decorator