import datetime
import enum
import typing
import dataclasses

from requests.adapters import HTTPAdapter
from requests.models import Response


class CircuitBreakerError(Exception):
    pass


class CircuitBreakerOpenedError(CircuitBreakerError):
    pass


@enum.unique
class State(enum.Enum):
    open = enum.auto()
    half_open = enum.auto()
    closed = enum.auto()


@dataclasses.dataclass
class FailureCounter:
    total_failures: int = 0
    last_failure_dt: typing.Optional[datetime.datetime] = None


# class CircuitBreakerProtocol(typing.Protocol):
#
#     @property
#     def state(self) -> State:
#         pass
#
#     def reset(self) -> None:
#         pass
#
#     def record_failure(self) -> None:
#         pass
#
#
# class CircuitBreakerStorageProtocol(typing.Protocol):
#
#     def increment_counter(self):
#         pass
#
#     def reset_counter(self):
#         pass
#
#     @property
#     def failure_counter(self) -> FailureCounter:
#         pass


class CircuitBreaker:

    def __init__(self, storage, failure_checker=None):
        self._storage: CircuitBreakerStorageProtocol = storage
        self._failure_threshold = 100
        self._failure_checker: typing.Optional[typing.Callable] = failure_checker

    @property
    def state(self) -> State:
        counter = self._storage.failure_counter
        if counter.total_failures > self._failure_threshold:
            if counter.last_failure_dt >= datetime.datetime.utcnow():
                return State.half_open
            else:
                return State.open
        return State.closed

    def reset(self):
        self._storage.reset_counter()

    def record_failure(self):
        self._storage.increment_counter()

    def __call__(self, func, func_args, func_kwargs):
        if self.state is State.open:
            raise CircuitBreakerError()
        try:
            result = func(*func_args, **func_kwargs)
        except Exception as e:
            result = e

        is_failure = self.check_failure(result)

        if is_failure:
            self.record_failure()
        else:
            self.reset()
        if isinstance(result, Exception):
            raise result
        return result

    @staticmethod
    def _default_failure_checker(result: typing.Any) -> bool:
        return isinstance(result, Exception)

    def check_failure(self, result):
        checker = self._failure_checker or self._default_failure_checker
        return checker(result)

    def add_failure_checker(self, func):
        self._failure_checker = func


class CircuitBreakerAdapter(HTTPAdapter):

    __attrs__ = HTTPAdapter.__attrs__ + ['circuit_breaker']

    def __init__(self, *args, circuit_breaker, **kwargs):
        self.circuit_breaker: CircuitBreaker = circuit_breaker
        super().__init__(*args, **kwargs)

    def send(self, *args, **kwargs):
        self.circuit_breaker.add_failure_checker(self.has_failure)
        return self.circuit_breaker(func=super().send, func_args=args, func_kwargs=kwargs)

    @staticmethod
    def has_failure(result) -> bool:
        if isinstance(result, ConnectionError):
            return True
        if isinstance(result, Response) and result.status_code >= 500:
            return True
        return False
