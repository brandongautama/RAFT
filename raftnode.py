import rpyc
import sys
import time
import threading
import random
from threading import Timer
import os

'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''
class RaftNode(rpyc.Service):


    """
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
    """
    def __init__(self, config, nodeNum, argPortNum):
        file  = open(config, 'r')
        lines = file.readlines()
        self.allNodesHost = []
        self.allNodesPort = []
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
                if curNode == nodeNum:
                    self.curNode = int(curNode)
                    item = hostAndPortPart.split(':')
                    self.host = item[0]
                    self.portNum = int(item[1])
        if self.portNum != int(argPortNum):
            sys.exit("Port Number given is not consistent")
        print("Node", self.curNode, "is running")



        self.currentState = "follower"
        self.currentTerm = 0
        self.votedFor = None
        self.currentLeader = None
        self.isTimeout = True
        self.totalVotesCount = 0
        self.leaderTimer = None
        self.followerTimer = None
        self.electionTimer = None
        self.candidateTimer = None
        # self.logfile = None
        if not os.path.exists('./tmp/'):
            os.makedirs('./tmp/')
        self.parseLogFile()

        mainThread = threading.Thread(target = self.run_server)
        mainThread.start()

    def updateLogFile(self):
        self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w') 
        self.logfile.write('Term: {0}\nVotedFor: {1}'.format(self.currentTerm, self.votedFor))
        self.logfile.flush()

    def parseLogFile(self):
        self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w+')
        lines = self.logfile.readlines()
        if lines:
            for line in lines:
                value = line.split(': ')
                if value[0] == 'Term':
                    self.currentTerm = int(value[1])
                elif value[0] == 'VotedFor':
                    self.votedFor = int(value[1])

    def run_server(self):
        while True:
            # time.sleep(random.randint(150, 301)/100)
            # self.currentState = 'leader'
            # print('node', self.curNode, 'term', self.currentTerm)
            # print('state', self.currentState)
            if self.currentState == 'leader':
                if self.leaderTimer == None:
                    self.leaderTimer = Timer(0.1, self.leaderAction)
                    self.leaderTimer.start()
            elif self.currentState == 'follower':
                # print(2)
                # time.sleep(random.randint(150, 301)/1000)
                if self.followerTimer == None:
                    self.followerTimer = Timer(random.randint(10000, 30001)/float(10000), self.followerAction)
                    self.followerTimer.start()
            elif self.currentState == 'candidate':
                # time.sleep(random.randint(150, 31)/1000)
                # print(1)
                if self.electionTimer == None:
                    self.electionTimer = Timer(random.randint(12000, 20001)/float(10000), self.setupElection)
                    self.electionTimer.start()
                if self.candidateTimer == None:
                    self.candidateTimer = Timer(.2, self.candidateAction)
                    self.candidateTimer.start()
            # time.sleep(0.1)


    def leaderAction(self):
        for i in range(0, self.numNodes):
            if i == self.curNode:
                continue
            args = (self.currentTerm, self.curNode, self.allNodesHost[i], self.allNodesPort[i])
            t = threading.Thread(target = self.sendHeartBeat, args= args)
            t.start()
        self.leaderTimer = None

    def followerAction(self):
        # if self.isTimeout == True:
        print('follwer','node', self.curNode, 'term', self.currentTerm)
        print('follower','state', self.currentState)
        self.currentState = 'candidate'
        self.electionTimer = None
        self.candidateTimer = None
        # print('follwer','node', self.curNode, 'term', self.currentTerm)
        # print('follower','state', self.currentState)
        self.setupElection()
        # elif self.isTimeout == False:
            # self.isTimeout = True

    def candidateAction(self):
        if self.candidateTimer != None:
            self.candidateTimer.cancel()
            self.candidateTimer = None
        print('node', self.curNode, 'term', self.currentTerm)
        print('state', self.currentState)
        print(self.totalVotesCount)
        if self.totalVotesCount > self.numNodes/2:
            if self.electionTimer != None:
                self.electionTimer.cancel()
                self.electionTimer = None
            self.currentState = 'leader'
            self.currentLeader = self.curNode
            if self.candidateTimer != None:
                self.candidateTimer.cancel()
                self.candidateTimer = None
            self.leaderAction()
        else:
            self.reSetupElection()

    def sendHeartBeat(self, currentTerm, curNode, host, portNum):
        try:
            conn = rpyc.connect(host, portNum)
            respSuccess, respTerm = conn.root.appendEntries(currentTerm, curNode)
            print('Sent HeartBeat to', portNum)
            if respSuccess == False:
                self.currentState = 'follower'
                self.currentTerm = respTerm
                self.votedFor = None
        except Exception:
            print("Node", portNum, "crashed")


    def exposed_appendEntries(self, term, leaderID):
        if term > self.currentTerm:
            self.followerTimer.cancel()
            self.followerTimer = None
            self.currentTerm = term
            self.currentState = 'follower'
            self.currentLeader = leaderID
            # self.isTimeout = False
            self.votedFor = None
            # self.logfile.seek(0)
            threading.Thread(target = self.updateLogFile)
            # self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w') 
            # self.logfile.write('Term: {0}\nVotedFor: {1}'.format(self.currentTerm, self.votedFor))
            # self.logfile.flush()
            return (True, self.currentTerm)
        elif term == self.currentTerm:
            self.followerTimer.cancel()
            self.followerTimer = None
            self.currentState = 'follower'
            self.currentLeader = leaderID
            self.isTimeout = False
            return (True, self.currentTerm)
        else:
            return (False, self.currentTerm)
    

    def setupElection(self):
        self.currentTerm += 1
        self.totalVotesCount = 0
        self.totalVotesCount += 1
        self.votedFor = self.curNode
        #self.logfile.seek(0)
        # self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w') 
        # self.logfile.write('Term: {0}\nVotedFor: {1}'.format(self.currentTerm, self.votedFor))
        # self.logfile.flush()
        threading.Thread(target = self.updateLogFile)
        if self.electionTimer != None:
            self.electionTimer.cancel()
            self.electionTimer = None
        if self.candidateTimer != None:
            self.candidateTimer.cancel()
            self.candidateTimer = None
        for i in range(0, self.numNodes):
            if i == self.curNode:
                continue
            args = (self.currentTerm, self.curNode, self.allNodesHost[i], self.allNodesPort[i])
            t = threading.Thread(target = self.startElection, args= args)
            t.start()

    def reSetupElection(self):
        for i in range(0, self.numNodes):
            if i == self.curNode:
                continue
            args = (self.currentTerm, self.curNode, self.allNodesHost[i], self.allNodesPort[i])
            t = threading.Thread(target = self.startElection, args= args)
            t.start()

    def startElection(self, currentTerm, curNode, host, portNum):
        try:
            conn = rpyc.connect(host, portNum)
            vote = conn.root.requestVote(currentTerm, curNode)
            if vote == True:
                self.totalVotesCount += 1
        except Exception:
            print("Node", portNum, "crashed")



    def exposed_requestVote(self, term, candidateID):
        if term > self.currentTerm:
            self.totalVotesCount = 0
            self.currentState = 'follower'
            if self.followerTimer != None:
                self.followerTimer.cancel()
                self.followerTimer = None
            self.currentTerm = term
            self.votedFor = candidateID
            #self.logfile.seek(0)
            # self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w') 
            # self.logfile.write('Term: {0}\nVotedFor: {1}'.format(self.currentTerm, self.votedFor))
            # self.logfile.flush()
            threading.Thread(target = self.updateLogFile)
            return True
        elif term == self.currentTerm:
            if self.votedFor == None:
                self.totalVotesCount = 0
                self.currentState = 'follower'
                if self.followerTimer != None:
                    self.followerTimer.cancel()
                    self.followerTimer = None
                self.currentTerm = term
                self.votedFor = candidateID
                #self.logfile.seek(0)
                # self.logfile = open('./tmp/log' + str(self.curNode) + '.txt', 'w') 
                # self.logfile.write('Term: {0}\nVotedFor: {1}'.format(self.currentTerm, self.votedFor))
                # self.logfile.flush()
                threading.Thread(target = self.updateLogFile)
                return True
            else:
                return False
        elif term < self.currentTerm:
            return False

    '''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
    '''
    def exposed_is_leader(self):
        return self.currentLeader == self.curNode


if __name__ == '__main__':
    from rpyc.utils.server import ThreadPoolServer
    nodeNum = sys.argv[2]
    port = sys.argv[3]
    server = ThreadPoolServer(RaftNode(sys.argv[1], nodeNum, port), port = int(port))
    server.start()
