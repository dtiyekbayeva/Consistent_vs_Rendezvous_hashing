import numpy as np
import matplotlib.pyplot as plt
import hashlib
import bisect
from collections import defaultdict
import time
from tabulate import tabulate
%pylab

#will be using md5 for hashing purposes
def md5(key):
    return long(hashlib.md5(str(key)).hexdigest(), 16)

#simplest rendezvous hashing implementation
class RendezVous(object):

    def __init__(self, ips=None):
        if ips is None:
            ips = []
        self.ips = ips
        self._hash = md5

    def add(self, ip):
        self.ips.append(ip)

    def remove(self, ip):
        self.ips.remove(ip)

    def select(self, key):
        high_score = -1
        winner = None
        for ip in self.ips:
            score = self._hash("%s-%s" % (str(ip), str(key)))
            if score > high_score:
                high_score, winner = score, ip

            elif score == high_score:
                high_score, winner = score, max(str(ip), str(winner))
        return winner

    def select1(self, key):
        high_score = -1
        winner = None
        for ip in self.ips:
            score = self._hash("%s-%s" % (str(ip), str(key)))
            if score > high_score:
                high_score, winner = score, ip

            elif score == high_score:
                high_score, winner = score, max(str(ip), str(winner))
        return winner


def _repl(name, index):
    return '%s:%d' % (name, index)

#simplest consistent hashing implementation with 200 nodes
class ConsistentHashing(object):
#using replicas which are virtual nodes on the circle to even out the distribution
    def __init__(self, ips=[], replicas=200):
        self._ips = {}
        self._hashed_ips = []
        self.replicas = replicas
        self._hash = md5

        for ip in ips:
            self.add(ip)


    def add(self, ip):
        for i in range(self.replicas):
            sip = _repl(ip, i)
            hashed = self._hash(sip)
            self._ips[hashed] = sip
            bisect.insort(self._hashed_ips, hashed)

    def remove(self, ip):
        for i in range(self.replicas):
            sip = _repl(ip, i)
            hashed = self._hash(sip)
            del self._ips[hashed]
            index = bisect.bisect_left(self._hashed_ips, hashed)
            del self._hashed_ips[index]

    def select(self, username):
        hashed = self._hash(username)
        start = bisect.bisect(self._hashed_ips, hashed,
                              hi=len(self._hashed_ips)-1)
        return self._ips[self._hashed_ips[start]].split(':')[0]

    def select1(self, username):
        hashed = self._hash(username)
        start = bisect.bisect(self._hashed_ips, hashed,
                              hi=len(self._hashed_ips)-1)
        return self._ips[self._hashed_ips[start]].split(':')[0]

#assignment methods
def run_test_original(servers, users):
    selection = defaultdict(list)
    list_5servers = []
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)
    smallest = users + 1
    biggest = 0
    for db in selection:
        size = len(selection[db])
        if size < smallest:
            smallest = size
        if size > biggest:
            biggest = size
        list_5servers.append([size, db])
    return list_5servers
#method for testing assignment when a server gets deleted
def run_test_remove_server(servers, users):
    servers.remove('server2')
    selection = defaultdict(list)
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)

    smallest = users + 1
    biggest = 0
    list_4servers = []
    for i, db in enumerate(selection):
        size = len(selection[db])
        if size < smallest:
            smallest = size
        if size > biggest:
            biggest = size
        list_4servers.append([size, db])
    return list_4servers
#method for testing assignment when a server gets added
def run_test_add_server(servers, users):
    servers.add('server2')
    selection = defaultdict(list)
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)

    smallest = users + 1
    list_add_back = []
    biggest = 0
    for i, db in enumerate(selection):
        size = len(selection[db])
        if size < smallest:
            smallest = size
        if size > biggest:
            biggest = size

        list_add_back.append([size, db])
    return list_add_back
#method to test to which server each user is mapped originally, after deleted a server and adding it back again
def run_test_user_assign(servers, users):
    selection = defaultdict(list)
    fin_list = []
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)
        fin_list.append([u, user_db, 0, 0])

    servers.remove('server2')
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)
        if fin_list[u][0]==u:
            fin_list[u][2]=user_db
    servers.add('server2')

    selection = defaultdict(list)
    for u in range(users):
        user_db = servers.select(u)
        selection[user_db].append(u)
        if fin_list[u][0]==u:
            fin_list[u][3]=user_db
    return fin_list


def cons_test_time(servers, users):
    cluster = ConsistentHashing(list(servers))
    start = time.time()
    run_test_original(cluster, users)
    run_test_remove_server(cluster, users)
    run_test_add_server(cluster, users)
    time_test = time.time() - start
    return time_test

def rend_test_time(servers, users):
    cluster = RendezVous(list(servers))
    start = time.time()
    run_test_original(cluster, users)
    run_test_remove_server(cluster, users)
    run_test_add_server(cluster, users)
    time_test = time.time() - start
    return time_test

#testing how much calculation time it would take to assign each user to the server
def lin_time(servers_list):
    time_cons = []
    time_rend = []
    for t in servers_list:
        time_cons.append(cons_test_time(['server' + str(i) for i in range(1, t)], users=100000))
        time_rend.append(rend_test_time(['server' + str(i) for i in range(1, t)], users=100000))
    plt.plot(servers_list, time_cons, label='consistent hashing', color='#ff0066')
    plt.plot(servers_list, time_rend, label='rendezvous hashing', color='#0099ff')
    plt.xlabel('Number of Servers')
    plt.ylabel('Time in seconds')
    plt.title("Assignment time for 100000 users")
    plt.legend()
    plt.show()

#plotting grapphs
def plot_graph_orig(users_number):
    graph1=run_test_original(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph2=run_test_original(RendezVous(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph1.sort(key=lambda l:l[1], reverse=False)
    graph2.sort(key=lambda l:l[1], reverse=False)

    barWidth = 0.35
    bars1 = []
    bars2 = []
    col = []
    for b in range(0, len(graph1)):
        bars1.append(graph1[b][0])
        bars2.append(graph2[b][0])
        col.append(graph1[b][1])

    # Set position of bar on X axis
    r1 = np.arange(len(bars1))
    r2 = [x + barWidth for x in r1]
    # Make the plot
    plt.bar(r1, bars1, color='#ff0066', width=barWidth, edgecolor='white', label='Consistent Hashing')
    plt.bar(r2, bars2, color='#0099ff', width=barWidth, edgecolor='white', label='Rendezvous Hashing')

    # Add xticks on the middle of the group bars
    plt.xlabel('')
    plt.xticks([r + barWidth for r in range(len(bars1))], [i for i in col])
    plt.ylabel(str(users_number) +  ' users', fontweight='bold')
    plt.title("Load Balancing for " + str(users_number) + " Users")
    plt.gca().legend(loc='upper center', bbox_to_anchor=(0.5, -0.08))
    plt.show()

def plot_graph_remove_serv(users_number):
    graph1=run_test_remove_server(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph2=run_test_remove_server(RendezVous(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph1.sort(key=lambda l:l[1], reverse=False)
    graph2.sort(key=lambda l:l[1], reverse=False)

    barWidth = 0.35
    bars1 = []
    bars2 = []
    col = []
    for b in range(0, len(graph1)):
        bars1.append(graph1[b][0])
        bars2.append(graph2[b][0])
        col.append(graph1[b][1])

    # Set position of bar on X axis
    r1 = np.arange(len(bars1))
    r2 = [x + barWidth for x in r1]
    # Make the plot
    plt.bar(r1, bars1, color='#ff0066', width=barWidth, edgecolor='white', label='Consistent Hashing')
    plt.bar(r2, bars2, color='#0099ff', width=barWidth, edgecolor='white', label='Rendezvous Hashing')

    # Add xticks on the middle of the group bars
    plt.xlabel('')
    plt.xticks([r + barWidth for r in range(len(bars1))], [i for i in col])
    plt.title("Load Balancing for " + str(users_number) + " Users (server 2 removed)")
    plt.ylabel(str(users_number) +  ' users', fontweight='bold')
    plt.gca().legend(loc='upper center', bbox_to_anchor=(0.5, -0.08))
    plt.show()

def plot_graph_add_server(users_number):
    graph1=run_test_add_server(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph2=run_test_add_server(RendezVous(list(['server' + str(i) for i in range(1, 6)])), users_number)
    graph1.sort(key=lambda l:l[1], reverse=False)
    graph2.sort(key=lambda l:l[1], reverse=False)

    barWidth = 0.35
    bars1 = []
    bars2 = []
    col = []
    for b in range(0, len(graph1)):
        bars1.append(graph1[b][0])
        bars2.append(graph2[b][0])
        col.append(graph1[b][1])

    # Set position of bar on X axis
    r1 = np.arange(len(bars1))
    r2 = [x + barWidth for x in r1]
    # Make the plot
    plt.bar(r1, bars1, color='#ff0066', width=barWidth, edgecolor='white', label='Consistent Hashing')
    plt.bar(r2, bars2, color='#0099ff', width=barWidth, edgecolor='white', label='Rendezvous Hashing')

    # Add xticks on the middle of the group bars
    plt.xlabel('')
    plt.xticks([r + barWidth for r in range(len(bars1))], [i for i in col])
    plt.ylabel(str(users_number) +  ' users', fontweight='bold')
    plt.title("Load Balancing for " + str(users_number) + " Users (server 2 added back)")
    plt.gca().legend(loc='upper center', bbox_to_anchor=(0.5, -0.08))
    plt.show()

for i in ([1000, 100000, 1000000]):
    plot_graph_orig(i)
    plot_graph_remove_serv(i)
    plot_graph_add_server(i)

#printing assignment tables for 10 users
def user_assign_print_table_cons():
    data = run_test_user_assign(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)])), 10)
    print "====consistent hashing===="
    print(tabulate(data, headers=["User", "5 servers", "server 2 removed", "server 2 added back"], tablefmt="grid"))

def user_assign_print_table_rend():
    data = run_test_user_assign(RendezVous(list(['server' + str(i) for i in range(1, 6)])), 10)
    print "====rendezvous hashing===="
    print(tabulate(data, headers=["User", "5 servers", "server 2 removed", "server 2 added back"], tablefmt="grid"))


#comparing only consistent hashing distribution with various number of virtual nodes
def plot_graph_cons_virtual_nodes_comp(users_number):
    graph1=run_test_original(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)]), replicas=10), users_number)
    graph2=run_test_original(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)]), replicas=100), users_number)
    graph3=run_test_original(ConsistentHashing(list(['server' + str(i) for i in range(1, 6)]), replicas=200), users_number)

    graph1.sort(key=lambda l:l[1], reverse=False)
    graph2.sort(key=lambda l:l[1], reverse=False)
    graph3.sort(key=lambda l:l[1], reverse=False)

    barWidth = 0.20
    bars1 = []
    bars2 = []
    bars3 = []
    col = []
    for b in range(0, len(graph1)):
        bars1.append(graph1[b][0])
        bars2.append(graph2[b][0])
        bars3.append(graph2[b][0])
        col.append(graph1[b][1])

    # Set position of bar on X axis
    r1 = np.arange(len(bars1))
    r2 = [x + barWidth for x in r1]
    r3 = [x + barWidth for x in r2]
    # Make the plot
    plt.bar(r1, bars1, color='#ff0066', width=barWidth, edgecolor='white', label='Consistent Hashing, Virtual nodes = 10')
    plt.bar(r2, bars2, color='#0099ff', width=barWidth, edgecolor='white', label='Consistent Hashing, Virtual nodes = 100')
    plt.bar(r3, bars3, color='#f5f542', width=barWidth, edgecolor='white', label='Consistent Hashing, Virtual nodes = 200')

    # Add xticks on the middle of the group bars
    plt.xlabel(str(users_number) +  ' users', fontweight='bold')
    plt.xticks([r + barWidth for r in range(len(bars1))], [i for i in col])
    plt.title("Load Balancing, server 2 removed")
    plt.xlabel('Number of Servers')
    plt.ylabel('Number of Users')
    plt.gca().legend(loc='center left', bbox_to_anchor=(1, 0.5))
    plt.show()


plot_graph_cons_virtual_nodes_comp(1000)
user_assign_print_table_cons()
user_assign_print_table_rend()
