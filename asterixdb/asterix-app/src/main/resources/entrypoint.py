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
from os import pathsep
addr = str(sys.argv[1])
port = str(sys.argv[2])
paths = sys.argv[3]
for p in paths.split(pathsep):
    sys.path.append(p)
from struct import *
import signal
import msgpack
import socket
from importlib import import_module
from pathlib import Path
from enum import IntEnum
from io import BytesIO

PROTO_VERSION = 1
HEADER_SZ = 8+8+1
REAL_HEADER_SZ = 4+8+8+1


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
    packer = msgpack.Packer(autoreset=False)
    unpacker = msgpack.Unpacker()
    response_buf = BytesIO()
    stdin_buf = BytesIO()
    wrapped_fns = {}
    alive = True

    def init(self, module_name, class_name, fn_name):
        self.wrapped_module = import_module(module_name)
        # do not allow modules to be called that are not part of the uploaded module
        wrapped_fn = None
        if not self.check_module_path(self.wrapped_module):
            wrapped_module = None
            raise ImportError("Module was not found in library")
        if class_name is not None:
            self.wrapped_class = getattr(
                import_module(module_name), class_name)()
        if self.wrapped_class is not None:
            wrapped_fn = getattr(self.wrapped_class, fn_name)
        else:
            wrapped_fn = locals()[fn_name]
        if wrapped_fn is None:
            raise ImportError("Could not find class or function in specified module")
        self.wrapped_fns[self.rmid] = wrapped_fn

    def nextTuple(self, *args, key=None):
        return self.wrapped_fns[key](*args)

    def check_module_path(self, module):
        cwd = Path('.').resolve()
        module_path = Path(module.__file__).resolve()
        return cwd in module_path.parents

    def read_header(self, readbuf):
        self.sz, self.mid, self.rmid, self.flag = unpack(
            "!iqqb", readbuf[0:21])
        return True

    def write_header(self, response_buf, dlen):
        total_len = dlen + HEADER_SZ
        header = pack("!iqqb", total_len, int(-1), int(self.rmid), self.flag)
        self.response_buf.write(header)
        return total_len+4

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

    def helo(self):
        #need to ack the connection back before sending actual HELO
        self.init_remote_ipc()

        self.flag = MessageFlags.NORMAL
        self.response_buf.seek(0)
        self.packer.pack(int(MessageType.HELO))
        self.packer.pack("HELO")
        dlen = 5 #tag(1) + body(4)
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
        args = self.unpacked_msg[1]
        result = None
        if args is None:
            result = self.nextTuple(key=self.rmid)
        else:
            result = self.nextTuple(args, key=self.rmid)
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(result)
        dlen = len(body)+1  # 1 for tag
        resp_len = self.write_header(self.response_buf, dlen)
        self.packer.pack(int(MessageType.CALL_RSP))
        self.response_buf.write(self.packer.bytes())
        self.response_buf.write(body)
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        return True

    def handle_error(self,e):
        self.flag = MessageFlags.NORMAL
        result = type(e).__name__ + ": " + str(e)
        self.packer.reset()
        self.response_buf.seek(0)
        body = msgpack.packb(result)
        dlen = len(body)+1  # 1 for tag
        resp_len = self.write_header(self.response_buf, dlen)
        self.packer.pack(int(MessageType.ERROR))
        self.response_buf.write(self.packer.bytes())
        self.response_buf.write(body)
        self.resp = self.response_buf.getbuffer()[0:resp_len]
        self.send_msg()
        self.packer.reset()
        return True

    type_handler = {
        MessageType.HELO: helo,
        MessageType.QUIT: quit,
        MessageType.INIT: handle_init,
        MessageType.CALL: handle_call
    }

    def connect_sock(self, addr, port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.connect((addr, int(port)))
        except socket.error as msg:
            print(sys.stderr, msg)

    def disconnect_sock(self, *args):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def recv_msg(self):
        completed = False
        while not completed and self.alive:
            readbuf = sys.stdin.buffer.read1(4096)
            try:
                if(len(readbuf) < REAL_HEADER_SZ):
                    while(len(readbuf) < REAL_HEADER_SZ):
                        readbuf += sys.stdin.buffer.read1(4096)
                self.read_header(readbuf)
                if(self.sz > len(readbuf)):
                    while(len(readbuf) < self.sz):
                        readbuf += sys.stdin.buffer.read1(4096)
                self.unpacker.feed(readbuf[21:])
                self.unpacked_msg = list(self.unpacker)
                self.type = MessageType(self.unpacked_msg[0])
                completed = self.type_handler[self.type](self)
            except BaseException as e:
                completed = self.handle_error(e)

    def send_msg(self):
        self.sock.sendall(self.resp)
        self.resp = None
        return

    def recv_loop(self):
        while self.alive:
            self.recv_msg()
        self.disconnect_sock()


wrap = Wrapper()
wrap.connect_sock(addr, port)
signal.signal(signal.SIGTERM, wrap.disconnect_sock)
wrap.recv_loop()
