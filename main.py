from abc import ABC
from contextlib import ExitStack
from functools import wraps
from typing import Callable

UNDEFINED = None
EMPTY_LIST = list()
EMPTY_DICT = dict()
EMPTY_TUPLE = tuple()


class SessionLocal:
    ...


class Depends(ABC):
    ...


class ContextDepends(Depends):
    def __init__(self, gen: Callable):
        self.gen = gen

    def __enter__(self):
        try:
            return next(self.gen)
        except StopIteration:
            raise RuntimeError("generator didn't yield") from None

    def __exit__(self, typ, value, traceback):
        if typ is None:
            try:
                return next(self.gen)
            except StopIteration:
                return False
            else:
                raise RuntimeError("generator didn't stop")
        else:
            if value is None:
                value = typ()
            try:
                self.gen.throw(typ, value, traceback)
            except StopIteration as exc:
                return exc is not value
            except Exception as exec:
                raise exec


class DependencyResolve:
    @staticmethod
    def get_args_mapping(callable):
        default_values = callable.__defaults__
        if default_values is UNDEFINED:
            return UNDEFINED
        argument_names = callable.__code__.co_varnames
        mapping = tuple(zip(argument_names[-len(default_values) :]
, default_values))
        return mapping

    @staticmethod
    def resolve_dependencies(deps, stack, yielded_values):
        for dep in deps:
            values = {}
            if dep["args_mapping_from_arg_function"]:
                values = {
                    y: yielded_values.get(y)

                    for y in [x[0] for x in dep["args_mapping_from_arg_function"]]
                }
            yielded_values[dep["argument"]] = stack.enter_context(
                dep["dependency"].__class__(dep["dependency"].gen(**values))
            )
        return yielded_values

    @classmethod
    def getting_deps(cls, func_):
        mapping = cls.get_args_mapping(func_)
        if mapping:
            deps = []
            for arg, value in reversed(mapping):
                if issubclass(value.__class__, Depends):
                    additional_deps, mapping_from_args = cls.getting_deps(
                        func_=value.gen
                    )
                    deps.extend(additional_deps)
                    deps.append(
                        {
                            "argument": arg,
                            "dependency": value,
                            "args_mapping_from_arg_function": mapping_from_args,
                        }
                    )

            return deps, mapping
        return EMPTY_LIST, EMPTY_DICT

    @classmethod
    def dependency_injection(cls, func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            deps = []
            yielded_values = {}
            with ExitStack() as stack:
                all_deps, mapping = cls.getting_deps(func_=func)
                deps.extend(all_deps)
                yielded_values = cls.resolve_dependencies(deps, stack, yielded_values)
                result = func(
                    *args,
                    **kwargs,
                    **{
                        x["argument"]: yielded_values[x["argument"]]
                        for x in all_deps[-len(mapping) :]

                    },
                )
            return result

        return wrapper


def gen_db_sec():

    print("fake before")
    yield "fake word"
    print("fake after")


def fake2():

    print("fake2")
    yield "some data"
    print("end fake2")


def gen_db(
    db: SessionLocal = ContextDepends(gen_db_sec),
    # dbfake2: SessionLocal = ContextDepends(fake2),
):
    try:
        print(db)
        print("db yield")
        yield "db1"
        print("commit")
    except Exception:
        print("exception and rollback")
        raise Exception
    finally:
        print("closed")


def gen_swap_schema(
    db: SessionLocal = ContextDepends(gen_db),
    # dbfake: SessionLocal = ContextDepends(gen_db_sec),
):
    try:
        print(f"clone schema {db}")
        # print(dbfake)
        print("swapped schema")
        yield True
    except Exception:
        print("exception and rollback")
        raise Exception
    finally:
        print("swapped cloned schema back")


@DependencyResolve.dependency_injection
def main(
    vega,
    schema_swap: bool = ContextDepends(gen_swap_schema),
    # db: SessionLocal = ContextDepends(gen_db),
):
    print("999 db queries")
    print(schema_swap)
    print(vega)
    # print(db)


main(1)