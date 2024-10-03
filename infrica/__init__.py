from functools import wraps
import inspect
import json
from ruamel.yaml import YAML
from pathlib import Path
import datetime
from attrs import field, define
from cattrs import GenConverter
from collections import defaultdict
import click
import sqlite3
from typing import Any
import pytimeparse
from cachetools import LRUCache
from rich.pretty import pprint
import sys


yaml=YAML(typ='safe')   # default, if not specfied, is 'rt' (round-trip)

conv = GenConverter()

sqlite3.register_adapter(datetime, datetime.datetime.isoformat)
sqlite3.register_converter("timestamp", lambda x: datetime.datetime.fromisoformat(x.decode('utf8')))

@conv.register_structure_hook
def deserialize_ttl(val: str | int, cls) -> datetime.timedelta:
    return datetime.timedelta(seconds=pytimeparse.parse(str(val)))

@define
class CacheConfig:
    ttl: datetime.timedelta = None
    shared: bool = False
    max_mem: int = sys.maxsize
    max_disk: int = None
    persistent: bool = False
    evicts: list[str] = field(factory=list)

def omittable_parentheses(maybe_decorator=None, /, allow_partial=False):
    """A decorator for decorators that allows them to be used without parentheses"""
    def decorator(func):
        @wraps(decorator)
        def wrapper(*args, **kwargs):
            if len(args) == 1 and callable(args[0]):
                if allow_partial:
                    return func(**kwargs)(args[0])
                elif not kwargs:
                    return func()(args[0])
            return func(*args, **kwargs)
        return wrapper
    if maybe_decorator is None:
        return decorator
    else:
        return decorator(maybe_decorator)


class Context:
    def __init__(self, cache_db=None, cache_config=None):
        self.dsn = ''
        if cache_config:
            d = conv.structure(yaml.load(Path(cache_config)), dict[str, CacheConfig])
            self.cache_config = defaultdict(CacheConfig, d)
        else:
            self.cache_config = defaultdict(CacheConfig)
        self.mem_cache: dict[tuple[Any, ...], TTLCache] = {}
        self.disk_cache = sqlite3.connect(cache_db, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
        self.schema()

    def schema(self):
        self.disk_cache.executescript('''
        create table if not exists cache(context text, operation text, parameters jsonb, value jsonb, expires_at timestamp);

        create index if not exists idx_cache_context on cache(context);
        create index if not exists idx_cache_operation on cache(operation);
        -- create index if not exists idx_cache_parameters on cache(parameters);
        create index if not exists idx_cache_expires_at on cache(expires_at);

        create unique index if not exists idx_cache_key on cache(context, operation, parameters);
        ''')

    def close(self):
        self.disk_cache.commit()
        self.disk_cache.close()


def try_db_cache(db, key, now, func):
    db.execute('delete from cache where expires_at <= ?', (now,))
    row = db.execute('''
        select value, expires_at
        from cache where
        context = ? and
        operation = ? and
        parameters = ?
        ''',
        (key[0], key[1], json.dumps(conv.unstructure(key[2]), sort_keys=True))
    ).fetchone()
    if not row:
        return None
    ret_type = inspect.signature(func).return_annotation
    if ret_type is inspect._empty:
        ret_type = None
    return (conv.structure(json.loads(row[0]), ret_type), row[1])

def try_mem_cache(cache, key, now):
    res = cache.get(key, None)
    if res is None:
        return None
    if res[1] <= now:
        del cache[key]
        return None
    return res

def unset_cache(ctx, ns, shared=False):
    if shared:
        ctx.disk_cache.execute('delete from cache where operation = ?', (ns,))
        del ctx.mem_cache[ns]
    else:
        # handle non-shared cache (remove only the one in context of current host)
        # TODO

def set_cache(ctx, key, val, exp):
    ctx.mem_cache[key[1]][key] = (val, exp)
    ctx.disk_cache.execute(
        '''
        insert into cache(context, operation, parameters, value, expires_at)
        values (?, ?, ?, ?, ?)
        on conflict do
        update set
            context = excluded.context,
            operation = excluded.operation,
            parameters = excluded.parameters,
            value = excluded.value,
            expires_at = excluded.expires_at
        ''',
        (
            key[0],
            key[1],
            json.dumps(conv.unstructure(key[2]), sort_keys=True),
            json.dumps(conv.unstructure(val), sort_keys=True),
            exp,
        )
    )
    


@omittable_parentheses(allow_partial=True)
def operation():
    def decorator(func):
        ns = []
        if func.__module__ != '__main__':
            ns.append(func.__module__)
        ns.append(func.__qualname__)
        ns = '.'.join(ns)

        @wraps(func)
        def wrap(*args, **kwargs):
            ctx = args[0]

            if not isinstance(ctx, Context):  # must wrap only operations (first arg is context)
                raise

            if ns not in ctx.cache_config:
                ctx.cache_config[ns] = CacheConfig()

            config = ctx.cache_config.get(ns)

            full_key = (ctx.dsn if not config.shared else None, ns, (args[1:], tuple(sorted(kwargs.items()))))

            if ns not in ctx.mem_cache:
                ctx.mem_cache[ns] = LRUCache(maxsize=config.max_mem)

            cache = ctx.mem_cache[ns]

            for eviction in config.evicts:
                unset_cache(ctx, eviction)

            now = datetime.datetime.now()
            hit = try_mem_cache(cache, full_key, now)
            is_hit_db = False
            if hit is None:
                is_hit_db = True
                hit = try_db_cache(ctx.disk_cache, full_key, now, func)
            if hit is None:  # not cached
                print(f'operation call: {ns}, cold cache')
                computed = func(*args, **kwargs)
                exp = now + config.ttl if config.ttl else datetime.datetime.max
                set_cache(ctx, full_key, computed, exp)
                return computed
            if is_hit_db:
                print(f'operation call: {ns}, db cache hit')
                cache[full_key] = hit
            else:
                print(f'operation call: {ns}, mem cache hit')
            return hit[0]
        return wrap
    return decorator


@define
class CustomResult:
    a: str
    b: str


@operation
def fn1(c: Context) -> CustomResult:
    print('fn1 called')
    return CustomResult('aaa', 'bbb')


@operation
def fn2(c: Context):
    print('fn2 called')


@click.command
@click.option('--cache', default=':memory:', help='path do sqlite3 file to use as cache')
@click.option('--cache-config', default=None, help='path to configuration yaml for cache')
def cli(cache, cache_config):
    c = Context(cache_db=cache, cache_config=cache_config)
    pprint(fn1(c))
    pprint(fn1(c))
    pprint(fn2(c))
    pprint(fn1(c))
    pprint(fn1(c))
    c.close()

if __name__ == '__main__':
    cli()
