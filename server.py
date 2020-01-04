#!/usr/bin/env python3

import sys
import time
import struct
import os
from threading import Thread, Timer, Event
from socket import socket, AF_INET, SOCK_DGRAM
from constants import *


class FileContext:
    # Context that is stored for each file transfer (read and write)
    def __init__(self, filename, block_num, timer, is_done=None):
        # file name to read from or write to
        self.filename = filename
        # last block read from or written to
        self.block_num = block_num
        # timer from previous exchange that retransmits the last msg once timeout is hit
        self.timer = timer
        # read-only field indicating the last client's ACK
        self.is_done = is_done


class Server:
    def __init__(self, socket, timeout):
        self.sock = socket
        self.timeout = timeout
        # self.reads and self.writes map: addr => FileContext
        # used to maintain state of read/write transfers
        self.reads = {}
        self.writes = {}

    def _transmit(self, pkt, addr):
        # sends packet to address over server's socket
        self.sock.sendto(pkt, addr)

    def _ACK(self, block_num):
        # constructs an ACK packet
        return struct.pack('>hh', ACK_MODE, block_num)

    def _DATA(self, block_num, buf):
        # constructs a DATA packet with data from buf
        return struct.pack('>hh%ds' % len(buf), DATA_MODE, block_num, buf)

    def _ERROR(self, code, msg):
        # constructs an ERROR packet with the supplied msg
        enc_msg = (msg+'\0').encode()
        return struct.pack('>hh%dsb' % len(enc_msg), ERROR_MODE, code, enc_msg, 0)

    def _handle_RRQ(self, addr, filename, block_num):
        # processes read transfer requests
        with open(filename, 'rb') as f:
            f.seek((block_num-1)*MAX_BYTES, os.SEEK_SET)
            buf = f.read(MAX_BYTES)

        # can determine if the last ACK if num bytes less than the max
        is_done = len(buf) < MAX_BYTES
        pkt = self._DATA(block_num, buf)
        timer = Event()
        self.reads[addr] = FileContext(filename, block_num, timer, is_done)

        print('Sent DATA ', block_num)
        self._transmit(pkt, addr)
        while not timer.wait(self.timeout):
            print('Retry DATA ', block_num)
            self._transmit(pkt, addr)

    def _handle_WRQ(self, addr, filename, block_num, buf=None):
        # processes write transfer requests
        is_done = False
        if block_num > 0:
            with open(filename, 'wb') as f:
                f.seek((block_num-1)*MAX_BYTES, os.SEEK_SET)
                f.write(buf)
            # can determine if the last DATA if num bytes less than the max
            is_done = len(buf) < MAX_BUFFER_WRQ

        pkt = self._ACK(block_num)
        timer = Event()
        self.writes[addr] = FileContext(filename, block_num, timer)

        print('Sent ACK ', block_num)
        self._transmit(pkt, addr)
        if not is_done:
            while not timer.wait(self.timeout):
                print('Retry ACK ', block_num)
                self._transmit(pkt, addr)

    def _handle_ACK(self, addr, block_num):
        self._validate_tid(addr, self.reads)
        fc = self.reads[addr]

        # Only process in-progress ACKS and with correct block_num
        # i.e. ignore out-of-order or duplicate ACKS
        if fc.block_num == block_num:
            fc.timer.set()
            print('Received ACK ', block_num)
            if not fc.is_done:
                self._handle_RRQ(addr, fc.filename, fc.block_num+1)

    def _handle_DATA(self, addr, block_num, buf):
        self._validate_tid(addr, self.writes)
        fc = self.writes[addr]

        # Only process in-progress DATAs and with correct block_num
        # i.e. ignore out-of-order or duplicate DATAs
        if fc.block_num+1 == block_num:
            fc.timer.set()
            print('Received DATA ', block_num)
            self._handle_WRQ(addr, fc.filename, fc.block_num+1, buf)

    def _handle_ERROR(self, addr, code, msg):
        pkt = self._ERROR(code, msg)
        print('Sent ERROR')
        sock.sendto(pkt, addr)

    def _parse_init_request(self, data):
        filename, mode = data.decode().split('\0')[:2]
        return filename, mode

    def _validate_mode(self, addr, mode):
        if mode != 'octet':
            self._handle_ERROR(addr, ERR_ILLEGAL_TFTP_OP,
                               'TFTP only supports binary mode')
            sys.exit()

    def _validate_file(self, addr, filename):
        if not os.path.exists(filename):
            self._handle_ERROR(addr, ERR_FILE_NOT_FOUND,
                               'File does not exist')
            sys.exit()

    def _validate_tid(self, addr, dct):
        if addr not in dct:
            self._handle_ERROR(addr, ERR_UNKNOWN_TID,
                               'Unknown transfer ID')
            sys.exit()

    def _parse_pkt(self, data, addr):
        # use first 2 bytes of message to parse the packet type
        mode, rest = struct.unpack('>h', data[:2])[0], data[2:]

        if mode == RRQ_MODE:
            # validate mode and if file exists
            filename, mode = self._parse_init_request(rest)
            self._validate_mode(addr, mode)
            self._validate_file(addr, filename)
            # in case of multiple RRQs in succession,
            # terminate any previous transfer's timeouts
            if addr in self.reads:
                self.reads[addr].timer.set()
            print('Received RRQ')

            block_num = 1
            self._handle_RRQ(addr, filename, block_num)

        elif mode == WRQ_MODE:
            # validate mode
            filename, mode = self._parse_init_request(rest)
            self._validate_mode(addr, mode)
            # in case of multiple WRQs in succession,
            # terminate any previous transfer's timeouts
            if addr in self.writes:
                self.writes[addr].timer.set()
            print('Received WRQ')

            block_num = 0
            self._handle_WRQ(addr, filename, block_num)

        elif mode == ACK_MODE:
            # unpack block num
            bnum = struct.unpack('>h', rest)[0]
            self._handle_ACK(addr, bnum)

        elif mode == DATA_MODE:
            # unpack block num and data
            bnum, data = struct.unpack('>h', rest[:2])[0], rest[2:]
            self._handle_DATA(addr, bnum, data)
        else:
            # unknown packet type
            self._handle_ERROR(addr, ERR_NOT_DEFINED,
                               'Invalid packet type')

    def run_server(self):
        # Main method of server; listens for incoming UDP requests
        # and farms them out to new threads for processing
        while True:
            data, addr = self.sock.recvfrom(MAX_BYTES)
            print('Client: ', addr)

            if data:
                t = Thread(target=self._parse_pkt, args=(data, addr))
                t.start()


if __name__ == '__main__':
    args = sys.argv
    if not (len(args) == 3 and args[1].isdigit() and args[2].isdigit()):
        print('Usage: server.py <port> <timeout>')
    else:
        # Initialize UDP socket with the given port
        port = int(args[1])
        sock = socket(AF_INET, SOCK_DGRAM)
        sock.bind(('', port))

        # Create server class with socket and timeout in ms
        timeout = int(args[2])/1000
        server = Server(sock, timeout)
        server.run_server()
