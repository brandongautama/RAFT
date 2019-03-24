import unittest
import rpyc
import sys
import threading
import os
import subprocess
import signal

class Client(unittest.TestCase):
    # def __init__(self):

        # file  = open('./config.txt', 'r')
        # lines = file.readlines()
        # self.allNodesHost = []
        # self.allNodesPort = []
        # for line in lines:
        #     value = line.split(': ')
        #     nodePart = value[0]
        #     hostAndPortPart = value[1]
        #     if nodePart == 'N':
        #         self.numNodes = int(value[1])
        #     elif nodePart[0:4:] == 'node':
        #         hostAndPortPart = hostAndPortPart.rstrip('\n')
        #         start = 'node'
        #         curNode = nodePart[nodePart.find(start)+len(start)::]
        #         cur = hostAndPortPart.split(':')
        #         self.allNodesHost.append(cur[0])
        #         self.allNodesPort.append(int(cur[1]))
        # self.test_1()

    def connect(self, host, port):
        conn = rpyc.connect(host, port)
        if conn.root.is_leader():
            self.leaders += 1
        print(self.leaders)


    # 5 nodes
    def test_1(self):
        file  = open('./config.txt', 'r')
        lines = file.readlines()
        self.allNodesHost = []
        self.allNodesPort = []
        self.pidlist = []
        for line in lines:
            value = line.split(': ')
            nodePart = value[0]
            hostAndPortPart = value[1]
            if nodePart == 'N':
                self.numNodes = int(value[1])
            elif nodePart[0:4:] == 'node':
                hostAndPortPart = hostAndPortPart.rstrip('\n')
                start = 'node'
                curNode = nodePart[nodePart.find(start)+len(start)::]
                cur = hostAndPortPart.split(':')
                self.allNodesHost.append(cur[0])
                self.allNodesPort.append(int(cur[1]))
        self.leaders = 0
        followers = 0

        # print(os.path.dirname(os.path.realpath(raftnode.py)))
        # # cwd=r
        for i in range(0, self.numNodes):
            port = int(i) + 5001
            cmd = 'python3 raftnode.py ./config.txt {0} {1}'.format(i, port)
            pro = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
            self.pidlist.append(pro.pid)

        for i in range(0, self.numNodes):
            args = (self.allNodesHost[i], self.allNodesPort[i])
            t = threading.Thread(target = self.connect, args= args)
            t.start()
        t.join()

        followers = self.numNodes - self.leaders
        print(self.leaders, followers)
        self.assertEqual(self.leaders, 1)
        self.assertEqual(followers, 5)

        for id in self.pidlist:
            os.killpg(os.getpgid(id), signal.SIGTERM)

    #4 nodes
    # def test_1(self):
    #     file  = open('./config.txt', 'r')
    #     lines = file.readlines()
    #     self.allNodesHost = []
    #     self.allNodesPort = []
    #     for line in lines:
    #         value = line.split(': ')
    #         nodePart = value[0]
    #         hostAndPortPart = value[1]
    #         if nodePart == 'N':
    #             self.numNodes = int(value[1])
    #         elif nodePart[0:4:] == 'node':
    #             hostAndPortPart = hostAndPortPart.rstrip('\n')
    #             start = 'node'
    #             curNode = nodePart[nodePart.find(start)+len(start)::]
    #             cur = hostAndPortPart.split(':')
    #             self.allNodesHost.append(cur[0])
    #             self.allNodesPort.append(int(cur[1]))
    #     self.leaders = 0
    #     followers = 0
    #     for i in range(0, self.numNodes):
    #         args = (self.allNodesHost[i], self.allNodesPort[i])
    #         t = threading.Thread(target = self.connect, args= args)
    #         t.start()
    #     t.join()
    #     followers = self.numNodes - self.leaders
    #     self.assertEqual(self.leaders, 1)
    #     self.assertEqual(followers, 4)

    # # 4 nodes
    #
    # def test1(self):
    #   self.leaders = 0
    #   followers = 0
    #   for i in range(0, self.numNodes):
    #         args = (self.allNodesHost[i], self.allNodesPort[i], self.leaders)
    #         t = threading.Thread(target = self.connect, args= args)
    #         t.start()
    #       leaders += 1
    #   followers = self.numNodes - self.leaders
    #     self.assertEqual(leaders, 1)
    #   self.assertEqual(followers, 4)

if __name__ == '__main__':
    unittest.main()
    # client = Client()
