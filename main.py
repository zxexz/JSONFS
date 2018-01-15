from fuse import FUSE, FuseOSError, Operations
from subprocess import call
from json import load as json_load, dumps
from stat import S_IFDIR, S_IFLNK, S_IFREG
from collections import defaultdict
from time import time
import os
import base64
from errno import ENOENT, EINVAL, EEXIST

FS_META = 'FSMETA'
FSDATA = '__FSDATA__'
EXTRA_ATTRS = 'EXTRAATTRS'
DATA = 'DATA'
LOOKUP_MAP = 'LOOKUPMAP'
ATTRS = 'ATTRS'
FILES = 'FILES'
FOLDERS = 'FOLDERS'
FILE_SYSTEM = 'FILESYSTEM'
ENTITY_TYPE = 'ENTITYTYPE'
DEFAULT_UMASK = 'DEFAULTUMASK'
ROOT = ''
SEP = '/'

ST_MODE = 'st_mode'
ST_MTIME = 'st_mtime'
ST_CTIME = 'st_ctime'
ST_ATIME = 'st_atime'
ST_NLINK = 'st_nlink'
ST_UID = 'st_uid'
ST_GID = 'st_gid'
ST_SIZE = 'st_size'


def dump_to_json(obj):
    obj_out = dict()
    for top_key, top_val in obj.items():
        if top_key not in {DATA, EXTRA_ATTRS}:
            if isinstance(top_val, dict):
                ascii_out = {top_key: {k: v for k, v in top_val.items() if k is not FSDATA}}
            else:
                ascii_out = {top_key: top_val}
            obj_out.update(ascii_out)
        else:
            ascii_out = dict()
            for inner_key, inner_val in top_val.items():
                if inner_key is not FSDATA:
                    try:
                        inner_dict = {inner_key: inner_val.decode('UTF-8')}
                    except:
                        inner_dict = {inner_key: [base64.b64encode(inner_val).decode('UTF-8')]}
                    ascii_out.update(inner_dict)
            obj_out.update({top_key: ascii_out})
    return dumps(obj_out, sort_keys=True, indent=2)


def dump_to_json_file(file_path, obj):
    with open(file_path, 'w') as file_handle:
        file_handle.write(dump_to_json(obj))


def load_from_json_file(file_path):
    return_dict = dict()
    with open(file_path) as file_handle:
        for top_key, top_val in json_load(file_handle).items():
            if top_key != DATA:
                return_dict.update({top_key: top_val})
            else:
                bytes_in = dict()
                for inner_key, inner_val in top_val.items():
                    inner_bytes = dict()
                    if not isinstance(inner_val, list):
                        inner_bytes.update({inner_key: inner_val.encode('UTF-8')})
                    else:
                        inner_bytes.update({inner_key: base64.b64decode(inner_val[0].encode('UTF-8'))})
                    bytes_in.update(inner_bytes)
                return_dict.update({top_key: bytes_in})
    return return_dict


def get_parent_dir(path):
    return os.path.dirname(os.path.dirname(path))


def is_valid_dirname(path):
    return path[-1] == SEP


def process_output(path):
    output = path if not path else path[0:-1] if path[-1] == SEP else path
    return output if not output else output[1::] if output[0] == SEP else output


class JSONFS(Operations):
    DEFAULT_UMASK = None  # type: int
    root_blob = None  # type: str
    fs_meta = None  # type: dict
    data = None  # type: dict
    attrs = None  # type: dict
    extra_attrs = None  # type: dict
    lookup_map = None  # type: dict
    file_system = None  # type: dict
    fd = None  # type: int

    def get_entity_type(self, path):
        return self.lookup_map.setdefault(process_output(path), dict()).setdefault(ENTITY_TYPE)

    def set_entity_type(self, path, entity_type):
        self.lookup_map[process_output(path)][ENTITY_TYPE] = entity_type

    def mkfs(self):
        created_time = time()
        root_node = {ST_MODE: (S_IFDIR | self.DEFAULT_UMASK),
                     ST_CTIME: created_time,
                     ST_MTIME: created_time,
                     ST_ATIME: created_time,
                     ST_NLINK: 2}
        file_system = {
            FS_META: {DEFAULT_UMASK: self.DEFAULT_UMASK},
            DATA: defaultdict(bytes),
            ATTRS: dict(),
            EXTRA_ATTRS: defaultdict(bytes),
            LOOKUP_MAP: defaultdict(dict)}
        file_system[ATTRS][ROOT] = root_node
        file_system[LOOKUP_MAP][ROOT] = {ENTITY_TYPE: FOLDERS}
        return file_system

    def __init__(self, root_blob=None, create_file_system=False, default_umask=os.umask(22)):
        self.fd = 0
        self.root_blob = os.path.abspath(os.path.dirname(root_blob))
        if not create_file_system:
            self.file_system = load_from_json_file(root_blob)
            self.DEFAULT_UMASK = self.file_system[FS_META][DEFAULT_UMASK]
        else:
            self.DEFAULT_UMASK = default_umask
            self.file_system = self.mkfs()
        self.root_blob = root_blob
        self.fs_meta = self.file_system[FS_META]
        self.attrs = self.file_system[ATTRS]
        self.extra_attrs = self.file_system.setdefault(EXTRA_ATTRS, dict())
        self.data = self.file_system.setdefault(DATA, dict())
        self.lookup_map = self.file_system[LOOKUP_MAP]
        self.create(FSDATA, 33204)

    def chmod(self, path, mode):
        path = process_output(path)
        self.attrs[path][ST_MODE] &= 0o770000
        self.attrs[path][ST_MODE] |= mode
        return 0

    def chown(self, path, uid, gid):
        path = process_output(path)
        self.attrs[path][ST_UID] = uid
        self.attrs[path][ST_GID] = gid
        return 0

    def create(self, path, mode, fi=None):
        path = process_output(path)
        if path in self.attrs and path is not FSDATA:
            raise FuseOSError(EINVAL)
        created_time = time()
        self.attrs[path] = {
            ST_MODE: (S_IFREG | mode),
            ST_NLINK: 1,
            ST_SIZE: 0,
            ST_CTIME: created_time,
            ST_MTIME: created_time,
            ST_ATIME: created_time
        }
        self.data[path] = bytes()
        self.lookup_map[path] = dict()
        self.set_entity_type(path, FILES)
        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        path = process_output(path)
        if path == FSDATA:
            self.write(path, dump_to_json(self.file_system).encode('UTF-8'), 0, 0)
        if path not in self.attrs:
            raise FuseOSError(ENOENT)
        return self.attrs[path]

    def getxattr(self, path, fh=None, **kwargs):
        path = process_output(path)
        if path not in self.attrs:
            raise FuseOSError(ENOENT)
        return self.extra_attrs.setdefault(path, bytes())

    def mkdir(self, path, mode):
        path = process_output(path)
        if path in self.attrs:
            raise FuseOSError(EEXIST)
        created_time = time()
        self.attrs[path] = {
            ST_MODE: (S_IFDIR | mode),
            ST_NLINK: 2,
            ST_SIZE: 0,
            ST_CTIME: created_time,
            ST_MTIME: created_time,
            ST_ATIME: created_time
        }
        self.attrs[process_output(get_parent_dir(path))][ST_NLINK] += 1
        self.set_entity_type(path, FOLDERS)
        return 0

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        path = process_output(path)
        return self.data[path][offset:offset + size]

    def readdir(self, path, fh):
        path = process_output(path)
        return_list = ['.', '..']
        for full_path in self.attrs.keys():
            out_item = process_output(full_path)
            if out_item and out_item != path and out_item[0:len(path)] == path:
                return_list.append(out_item)
        return return_list

    def rename(self, old, new):
        old = process_output(old)
        new = process_output(new)
        if old not in self.attrs:
            raise FuseOSError(ENOENT)
        if new in self.attrs:
            raise FuseOSError(EEXIST)
        self.attrs[new] = self.attrs.pop(old)
        self.extra_attrs[new] = self.extra_attrs.pop(old)
        self.extra_attrs[new] = self.extra_attrs.pop(old)
        self.data[new] = self.extra_attrs.pop(old)
        return 0

    def rmdir(self, path):
        # TODO: ADD CHECKS
        path = process_output(path)
        self.attrs.pop(path)
        self.lookup_map.pop(path)
        self.extra_attrs.pop(path)
        self.data.pop(path)
        self.attrs[get_parent_dir(path)][ST_NLINK] -= 1

    def statfs(self, path):
        path = process_output(path)
        return self.attrs[path]

    def symlink(self, target, source):
        # TODO: ADD CHECKS
        self.attrs[target] = {ST_MODE: S_IFLNK | 0o777,
                              ST_NLINK: 1,
                              ST_SIZE: len(source)}
        ent_type = self.get_entity_type(source)
        self.set_entity_type(target, ent_type)
        return 0

    def truncate(self, path, length, fh=None):
        # TODO: ADD CHECKS
        path = process_output(path)
        if path not in self.attrs:
            raise FuseOSError(ENOENT)
        self.data[path] = self.data[path][:length]
        self.attrs[path][ST_SIZE] = length
        return 0

    def unlink(self, path):
        path = process_output(path)
        if path not in self.attrs:
            raise FuseOSError(ENOENT)
        self.data.pop(path)
        self.attrs.pop(path)
        self.extra_attrs.pop(path)
        self.lookup_map.pop(path)
        return 0

    def utimens(self, path, times=None):
        path = process_output(path)
        if path not in self.attrs:
            raise FuseOSError(ENOENT)
        created_time = time()
        atime, mtime = times if times else (created_time, created_time)
        self.attrs[path][ST_ATIME] = atime
        self.attrs[path][ST_MTIME] = mtime
        return 0

    def write(self, path, data, offset, fh):
        path = process_output(path)
        self.data[path] = self.data[path][:offset] + data
        self.attrs[path][ST_SIZE] = len(self.data[path])
        return len(data)


def main():
    root_blob = 'goat.json'
    call(['fusermount', '-u', 'mountdir'])
    FUSE(JSONFS(root_blob, create_file_system=False), 'mountdir', nothreads=True, foreground=True)


if __name__ == '__main__':
    main()
