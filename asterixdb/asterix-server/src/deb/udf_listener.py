#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import sys
from systemd.daemon import listen_fds
from os import chdir
from os import getcwd
from os import getpid
from struct import *
import signal
import msgpack
import socket
import traceback
from importlib import import_module
from pathlib import Path
from enum import IntEnum
from io import BytesIO


PROTO_VERSION = 1
HEADER_SZ = 8 + 8 + 1
REAL_HEADER_SZ = 4 + 8 + 8 + 1
FRAMESZ = 32768


class MessageType(IntEnum):
    HELO = 0
    QUIT = 1
    INIT = 2
    INIT_RSP = 3
    CALL = 4
    CALL_RSP = 5
    ERROR = 6


class MessageFlags(IntEnum):
    NORMAL = 0
    INITIAL_REQ = 1
    INITIAL_ACK = 2
    ERROR = 3


class Wrapper(object):
    wrapped_module = None
    wrapped_class = None
    wrapped_fn = None
    sz = None
    mid = None
    rmid = None
    flag = None
    resp = None
    unpacked_msg = None
    msg_type = None
    packer = msgpack.Packer(autoreset=False, use_bin_type=False)
    unpacker = msgpack.Unpacker(raw=False)
    response_buf = BytesIO()
    stdin_buf = BytesIO()
    wrapped_fns = {}
    alive = True
    readbuf = bytearray(FRAMESZ)
    readview = memoryview(readbuf)


    def init(self, module_name, class_name, fn_name):
        self.wrapped_module = import_module(module_name)
        # do not allow modules to be called that are not part of the uploaded module
        wrapped_fn = None
        if not self.check_module_path(self.wrapped_module):
            self.wrapped_module = None
            raise ImportError("Module was not found in library")
        if class_name is not None:
            self.wrapped_class = getattr(
                import_module(module_name), class_name)()
        if self.wrapped_class is not None:
            wrapped_fn = getattr(self.wrapped_class, fn_name)
        else:
            wrapped_fn = getattr(import_module(module_name), fn_name)
        if wrapped_fn is None:
            raise ImportError(
                "Could not find class or function in specified module")
        self.wrapped_fns[self.mid] = wrapped_fn

    def next_tuple(self, *args, key=None):
        return self.wrapped_fns[key](*args)

    def check_module_path(self, module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents
        return True

    def read_header(self, readbuf):
        self.sz, self.mid, self.rmid, self.flag = unpack(
            "!iqqb", readbuf[0:REAL_HEADER_SZ])
        return True

    def write_header(self, response_buf, dlen):
        total_len = dlen + HEADER_SZ
        header = pack("!iqqb", total_len, int(-1), int(self.rmid), self.flag)
        self.response_buf.write(header)
        return total_len + 4

    def get_ver_hlen(self, hlen):
        return hlen + (PROTO_VERSION << 4)

    def get_hlen(self):
        return self.ver_hlen - (PROTO_VERSION << 4)

    def init_remote_ipc(self):
        self.response_buf.seek(0)
        self.flag = MessageFlags.INITIAL_REQ
        dlen = len(self.unpacked_msg[1])
        resp_len = self.write_header(self.response_buf, dlen)
        self.response_buf.write(self.unpacked_msg[1])
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()

    def cd(self, basedir):
        chdir(basedir + "/site-packages")
        sys.path.insert(0,getcwd())

    def helo(self):
        # need to ack the connection back before sending actual HELO
        #   self.init_remote_ipc()
        self.cd(self.unpacked_msg[1][1])
        self.flag = MessageFlags.NORMAL
        self.response_buf.seek(0)
        self.packer.pack(int(MessageType.HELO))
        self.packer.pack(int(getpid()))
        dlen = len(self.packer.bytes())  # tag(1) + body(4)
        resp_len = self.write_header(self.response_buf, dlen)
        self.response_buf.write(self.packer.bytes())
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        return True

    def handle_init(self):
        self.flag = MessageFlags.NORMAL
        self.response_buf.seek(0)
        args = self.unpacked_msg[1]
        module = args[0]
        if len(args) == 3:
            clazz = args[1]
            fn = args[2]
        else:
            clazz = None
            fn = args[1]
        self.init(module, clazz, fn)
        self.packer.pack(int(MessageType.INIT_RSP))
        dlen = 1  # just the tag.
        resp_len = self.write_header(self.response_buf, dlen)
        self.response_buf.write(self.packer.bytes())
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        return True

    def quit(self):
        self.alive = False
        return True

    def handle_call(self):
        self.flag = MessageFlags.NORMAL
        result = ([], [])
        if len(self.unpacked_msg) > 1:
            args = self.unpacked_msg[1]
            if args is not None:
                for arg in args:
                    try:
                        result[0].append(self.next_tuple(*arg, key=self.mid))
                    except BaseException as e:
                        result[1].append(traceback.format_exc())
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(result)
        dlen = len(body) + 1  # 1 for tag
        resp_len = self.write_header(self.response_buf, dlen)
        self.packer.pack(int(MessageType.CALL_RSP))
        self.response_buf.write(self.packer.bytes())
        self.response_buf.write(body)
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        return True

    def handle_error(self, e):
        self.flag = MessageFlags.NORMAL
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(str(e))
        dlen = len(body) + 1  # 1 for tag
        resp_len = self.write_header(self.response_buf, dlen)
        self.packer.pack(int(MessageType.ERROR))
        self.response_buf.write(self.packer.bytes())
        self.response_buf.write(body)
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        self.alive = False
        return True

    type_handler = {
        MessageType.HELO: helo,
        MessageType.QUIT: quit,
        MessageType.INIT: handle_init,
        MessageType.CALL: handle_call
    }

    def connect_sock(self):
        self.sock = socket.fromfd(listen_fds()[0], socket.AF_UNIX, socket.SOCK_STREAM)

    def disconnect_sock(self, *args):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def recv_msg(self):
        while self.alive:
            pos = self.sock.recv_into(self.readbuf)
            if pos <= 0:
                self.alive = False
                return
            try:
                while pos < REAL_HEADER_SZ:
                    read = self.sock.recv_into(self.readview[pos:])
                    if read <= 0:
                        self.alive = False
                        return
                    pos += read
                self.read_header(self.readview)
                while pos < self.sz and len(self.readbuf) - pos > 0:
                    read = self.sock.recv_into(self.readview[pos:])
                    if read <= 0:
                        self.alive = False
                        return
                    pos += read
                while pos < self.sz:
                    vszchunk = self.sock.recv(4096)
                    if len(vszchunk) == 0:
                        self.alive = False
                        return
                    self.readview.release()
                    self.readbuf.extend(vszchunk)
                    self.readview = memoryview(self.readbuf)
                    pos += len(vszchunk)
                self.unpacker.feed(self.readview[REAL_HEADER_SZ:self.sz])
                self.unpacked_msg = list(self.unpacker)
                self.msg_type = MessageType(self.unpacked_msg[0])
                self.type_handler[self.msg_type](self)
            except BaseException as e:
                self.handle_error(''.join(traceback.format_exc()))

    def send_msg(self):
        self.sock.sendall(self.resp)
        self.resp = None
        return

    def recv_loop(self):
        while self.alive:
            self.recv_msg()
        self.disconnect_sock()


wrap = Wrapper()
wrap.connect_sock()
signal.signal(signal.SIGTERM, wrap.disconnect_sock)
wrap.recv_loop()
