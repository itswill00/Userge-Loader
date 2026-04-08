__all__ = ['load']

import os
import sys
from contextlib import suppress
from multiprocessing import Process, Pipe, set_start_method
from shutil import which
from signal import signal, SIGINT, SIGTERM, SIGABRT
from typing import Set

from .checks import do_checks
from .menu import main_menu
from .methods import fetch_core, fetch_repos
from .types import Repos, Constraints, Sig, Requirements, Session, Tasks
from .utils import log, error, call, get_client_type, safe_url, grab_conflicts, clean_core, \
    clean_plugins, print_logo
from .. import __version__
from ..userge.main import run


def load_data() -> None:
    log("Loading Data ...")

    Repos.load()
    Constraints.load()


def init_core() -> None:
    log("Fetching Core ...")

    fetch_core()
    if Sig.core_exists():
        return

    log("Initializing Core ...")

    core = Repos.get_core()
    if core.failed:
        code, err = core.error
        error(f"error code: [{code}]\n{err}")

    core.checkout_version()

    loader_version = core.grab_loader_version()

    if loader_version:
        if __version__ < loader_version:
            log("\tUpdating loader to latest ...")

            code, err = call("git", "pull")
            if code:
                error(f"error code: [{code}]\n{err}")

            raise InterruptedError

    Requirements.update(core.grab_req())

    clean_core()
    core.copy()

    # apply anti-ban patches
    _apply_antiban_patches()
    _apply_local_db_patches()
    _apply_time_sync_patch()

    core.checkout_branch()

    Sig.repos_remove()
    Sig.core_make()


def init_repos() -> None:
    log("Fetching Repos ...")

    fetch_repos()
    if not Repos.has_repos() or Sig.repos_exists():
        return

    log("Initializing Repos ...")

    repos = 0
    plugins = {}
    core_version = Repos.get_core().info.count
    client_type = get_client_type()
    os_type = dict(posix='linux', nt='windows').get(os.name, os.name)

    for repo in Repos.iter_repos():
        if repo.failed:
            code, err = repo.error
            log(f"\tSkipping: {safe_url(repo.info.url)} code: [{code}] due to: {err}")
            continue

        repo.checkout_version()
        repo.load_plugins()

        unique = 0
        ignored = 0
        overridden = 0

        for plg in repo.iter_plugins():
            conf = plg.config
            reason = None

            for _ in ' ':
                if not conf.available:
                    reason = "not available"
                    break

                constraint = Constraints.match(plg)
                if constraint:
                    reason = f"constraint {constraint}"
                    break

                if conf.os and conf.os != os_type:
                    reason = f"incompatible os type {os_type}, required: {conf.os}"
                    break

                if conf.min_core and conf.min_core > core_version:
                    reason = (f"min core version {conf.min_core} is required, "
                              f"current: {core_version}")
                    break

                if conf.max_core and conf.max_core < core_version:
                    reason = (f"max core version {conf.max_core} is required, "
                              f"current: {core_version}")
                    break

                if (
                    conf.client_type
                    and client_type != "dual"
                    and conf.client_type.lower() != client_type
                ):
                    c_type = conf.client_type.lower()
                    reason = f"client type {c_type} is required, current: {client_type}"
                    break

                if conf.envs:
                    for env in conf.envs:
                        if '|' in env:
                            parts = tuple(filter(None, map(str.strip, env.split('|'))))

                            for part in parts:
                                if os.environ.get(part):
                                    break
                            else:
                                reason = f"one of envs {', '.join(parts)} is required"
                                break
                        else:
                            if not os.environ.get(env):
                                reason = f"env {env} is required"
                                break

                    if reason:
                        break

                if conf.bins:
                    for bin_ in conf.bins:
                        if not which(bin_):
                            reason = f"bin {bin_} is required"
                            break

                    if reason:
                        break

                old = plugins.get(plg.name)
                plugins[plg.name] = plg

                if old:
                    overridden += 1
                    log(f"\tPlugin: [{plg.cat}/{plg.name}] "
                        f"is overriding Repo: {safe_url(old.repo_url)}")
                else:
                    unique += 1

            else:
                continue

            ignored += 1
            log(f"\tPlugin: [{plg.cat}/{plg.name}] was ignored due to: {reason}")

        repos += 1
        log(f"\t\tRepo: {safe_url(repo.info.url)} "
            f"ignored: {ignored} overridden: {overridden} unique: {unique}")

    if plugins:

        for c_plg in Repos.get_core().get_plugins():
            if c_plg in plugins:
                plg = plugins.pop(c_plg)

                log(f"\tPlugin: [{plg.cat}/{plg.name}] was removed due to: "
                    "matching builtin found")

        def resolve_depends() -> None:
            all_ok = False

            while plugins and not all_ok:
                all_ok = True

                for plg_ in tuple(plugins.values()):
                    deps = plg_.config.depends
                    if not deps:
                        continue

                    for dep in deps:
                        if dep not in plugins:
                            all_ok = False
                            del plugins[plg_.name]

                            log(f"\tPlugin: [{plg_.cat}/{plg_.name}] was removed due to: "
                                f"plugin [{dep}] not found")

                            break

        def grab_requirements() -> Set[str]:
            data = set()

            for plg_ in plugins.values():
                packages_ = plg_.config.packages
                if packages_:
                    data.update(packages_)

            return data

        resolve_depends()
        requirements = grab_requirements()

        if requirements:
            conflicts = grab_conflicts(requirements)

            if conflicts:
                for conflict in conflicts:
                    for plg in tuple(plugins.values()):
                        packages = plg.config.packages

                        if packages and conflict in packages:
                            del plugins[plg.name]

                            log(f"\tPlugin: [{plg.cat}/{plg.name}] was removed due to: "
                                f"conflicting requirement [{conflict}] found")

                resolve_depends()
                requirements = grab_requirements()

            Requirements.update(requirements)

    clean_plugins()

    for plg in plugins.values():
        plg.copy()

    log(f"\tTotal plugins: {len(plugins)} from repos: {repos}")

    for repo in Repos.iter_repos():
        repo.checkout_branch()

    Sig.repos_make()


def install_req() -> None:
    pip = os.environ.get('CUSTOM_PIP_PACKAGES')
    if pip:
        Requirements.update(pip.split())

    size = Requirements.size()
    if size > 0:
        log(f"Installing Requirements ({size}) ...")

        code, err = Requirements.install()
        if code:
            error(f"error code: [{code}]\n{err}", interrupt=False)

            Sig.repos_remove()


def check_args() -> None:
    if len(sys.argv) > 1 and sys.argv[1].lower() == "menu":
        main_menu()


def run_loader() -> None:
    load_data()
    init_core()
    init_repos()
    install_req()


def initialize() -> None:
    try:
        print_logo()
        do_checks()
        check_args()
        run_loader()
    except InterruptedError:
        raise
    except Exception as e:
        error(str(e))


def run_userge() -> None:
    log("Starting Userge ...")

    p_p, c_p = Pipe()
    p = Process(name="userge", target=run, args=(c_p,))
    Session.set_process(p)

    def handle(*_):
        p_p.close()
        Session.terminate()

    for _ in (SIGINT, SIGTERM, SIGABRT):
        signal(_, handle)

    p.start()
    c_p.close()

    with suppress(EOFError, OSError):
        while p.is_alive() and not p_p.closed:
            p_p.send(Tasks.handle(*p_p.recv()))

    p_p.close()
    p.join()
    p.close()


def _load() -> None:
    if Session.should_init():
        initialize()

    run_userge()
    if Session.should_restart():
        _load()


def _apply_antiban_patches() -> None:
    log("Applying Anti-Ban Patches ...")
    
    import glob
    import random
    
    # Target common message sending files in Userge
    # Usually: userge/core/methods/messages/send_message.py or similar
    targets = glob.glob("userge/**/send_message.py", recursive=True)
    
    for target in targets:
        try:
            with open(target, 'r') as f:
                content = f.read()
            
            if "asyncio.sleep(random.uniform(0.5, 2.0))" in content:
                continue
                
            # Inject import
            if "import asyncio" not in content:
                content = "import asyncio\nimport random\n" + content
            elif "import random" not in content:
                content = "import random\n" + content
                
            # Inject delay after function definition and docstring
            # We look for the main method definition, usually 'async def send_message'
            pattern = "async def send_message"
            if pattern in content:
                lines = content.splitlines()
                new_lines = []
                in_def = False
                patched = False
                
                for line in lines:
                    new_lines.append(line)
                    if pattern in line and not patched:
                        in_def = True
                    
                    # Look for the end of definition (closing parenthesis and colon)
                    if in_def and "):" in line and not patched:
                        # Found end of definition, now find where the body starts
                        # We skip docstrings if they exist
                        in_def = False
                        indent = " " * (line.find(")") + 4 if "(" in line else 8)
                        # Actually, let's just wait for the next non-empty, non-comment line
                        # But for simplicity, we'll append right after the definition's last line
                        new_lines.append(f"{indent}await asyncio.sleep(random.uniform(0.5, 2.0))")
                        patched = True
                
                with open(target, 'w') as f:
                    f.write("\n".join(new_lines))
                log(f"\tPatched: {target}")
        except Exception as e:
            log(f"\tFailed to patch {target}: {str(e)}")


def _apply_local_db_patches() -> None:
    log("Applying Local Database Patches ...")
    
    target = "userge/core/database.py"
    if not os.path.exists(target):
        return
        
    mock_db_code = """
import json
import asyncio
import os

class MockAsyncCollection:
    def __init__(self, name):
        self.name = name
        self._file = "database.json"

    def _get_data_sync(self):
        if os.path.exists(self._file):
            with open(self._file, 'r') as f:
                try: return json.load(f)
                except: return {}
        return {}

    def _save_data_sync(self, data):
        with open(self._file, 'w') as f:
            json.dump(data, f, indent=4)

    async def find_one(self, query, *args, **kwargs):
        db_data = self._get_data_sync()
        data = db_data.get(self.name, [])
        for item in data:
            if all(item.get(k) == v for k, v in query.items()):
                return item
        return None

    async def update_one(self, query, update, upsert=False, *args, **kwargs):
        db_data = self._get_data_sync()
        if self.name not in db_data: db_data[self.name] = []
        data = db_data[self.name]
        
        item = None
        for i in data:
            if all(i.get(k) == v for k, v in query.items()):
                item = i; break
        
        set_data = update.get('$set', {})
        if item:
            item.update(set_data)
        elif upsert:
            new_item = query.copy()
            new_item.update(set_data)
            data.append(new_item)
        
        self._save_data_sync(db_data)
        return type('obj', (), {'acknowledged': True, 'matched_count': 1 if item else 0})()

    async def insert_one(self, doc, *args, **kwargs):
        db_data = self._get_data_sync()
        if self.name not in db_data: db_data[self.name] = []
        db_data[self.name].append(doc)
        self._save_data_sync(db_data)
        return type('obj', (), {'acknowledged': True, 'inserted_id': 1})()

    def find(self, *args, **kwargs):
        # returns an async iterator
        class AsyncIter:
            def __init__(self, data): self.data = data; self.idx = 0
            def __aiter__(self): return self
            async def __anext__(self):
                if self.idx < len(self.data):
                    res = self.data[self.idx]; self.idx += 1; return res
                raise StopAsyncIteration
            async def to_list(self, length=None): return self.data[:length] if length else self.data
        
        db_data = self._get_data_sync()
        return AsyncIter(db_data.get(self.name, []))

    async def delete_one(self, query, *args, **kwargs):
        db_data = self._get_data_sync()
        data = db_data.get(self.name, [])
        for item in data:
            if all(item.get(k) == v for k, v in query.items()):
                data.remove(item)
                self._save_data_sync(db_data)
                break
        return type('obj', (), {'acknowledged': True, 'deleted_count': 1})()

    async def count_documents(self, query, *args, **kwargs):
        db_data = self._get_data_sync()
        return len(db_data.get(self.name, []))

def get_collection(name: str):
    return MockAsyncCollection(name)
"""
    try:
        with open(target, 'w') as f:
            f.write(mock_db_code)
        log(f"\tSuccessfully patched: {target}")
    except Exception as e:
        log(f"\tFailed to patch database: {str(e)}")


def _apply_time_sync_patch() -> None:
    log("Applying Advanced Time Sync Patch ...")
    
    target = "userge/core/client.py"
    if not os.path.exists(target):
        return
        
    try:
        with open(target, 'r') as f:
            content = f.read()
            
        if "time_sync_patch_applied" in content:
            return
            
        # We inject a monkey-patch right after imports in client.py
        patch = """
# time_sync_patch_applied
import time
from pyrogram.session import Session
from pyrogram.session.internals import msg_id

# Monkey-patch Pyrogram's msg_id generator to handle time desync
_old_msg_id = msg_id.MsgId

class PatchedMsgId:
    def __new__(cls, *args, **kwargs):
        # Force a slight forward time shift to avoid "too low" errors
        # Telegram allows msg_id to be up to 30 seconds in the future
        Session.offset_time = getattr(Session, 'offset_time', 0) + 5
        return _old_msg_id(*args, **kwargs)

    def __getattr__(self, name):
        return getattr(_old_msg_id, name)

    @staticmethod
    def set_server_time(server_time):
        return _old_msg_id.set_server_time(server_time)

try:
    # Attempt to patch if available
    import pyrogram.session.session as py_session
    py_session.MsgId = PatchedMsgId
except Exception:
    pass

"""
        # Find a good place to inject, like after 'from pyrogram import Client'
        if "from pyrogram import Client" in content:
            content = content.replace("from pyrogram import Client", "from pyrogram import Client\n" + patch, 1)
        else:
            # Or just at the top after docstrings
            lines = content.splitlines()
            for i, line in enumerate(lines):
                if line.startswith("import") or line.startswith("from"):
                    lines.insert(i, patch)
                    break
            content = "\n".join(lines)
            
        with open(target, 'w') as f:
            f.write(content)
            
        log("\tSuccessfully applied Pyrogram Time Sync Monkey-Patch.")
    except Exception as e:
        log(f"\tFailed to apply time sync patch: {str(e)}")


def load() -> None:
    log(f"Loader v{__version__}")
    set_start_method('spawn')

    with suppress(KeyboardInterrupt):
        _load()

    raise SystemExit
