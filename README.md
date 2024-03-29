# DistributedSystems

- [ ] efficiency
- [ ] lab4b bug

Lab1: MapReduce  

*** Starting wc test.  
--- wc test: PASS  
--- indexer test: PASS  
--- map parallelism test: PASS  
--- reduce parallelism test: PASS  
--- job count test: PASS  
--- early exit test: PASS  
--- crash test: PASS  
*** PASSED ALL TESTS  

Lab2: Raft  

Test (2A): initial election ...  
  ... Passed --   3.0  3  112   28802    0  
Test (2A): election after network failure ...  
  ... Passed --   4.6  3  228   44028    0  
Test (2A): multiple elections ...  
  ... Passed --   5.5  7 1092  210410    0  
Test (2B): basic agreement ...  
  ... Passed --   0.6  3   16    4072    3  
Test (2B): RPC byte count ...  
  ... Passed --   1.4  3   48  113012   11  
Test (2B): agreement despite follower disconnection ...  
  ... Passed --   5.5  3  206   51751    8  
Test (2B): no agreement if too many followers disconnect ...  
  ... Passed --   3.3  5  320   67120    3  
Test (2B): concurrent Start()s ...  
  ... Passed --   0.7  3   16    4156    6  
Test (2B): rejoin of partitioned leader ...  
  ... Passed --   4.0  3  252   57056    4  
Test (2B): leader backs up quickly over incorrect follower logs ...  
  ... Passed --  16.9  5 2468 2880597  102  
Test (2B): RPC counts aren't too high ...  
  ... Passed --   2.4  3   84   22292   12  
Test (2C): basic persistence ...  
  ... Passed --   3.4  3  114   27880    6  
Test (2C): more persistence ...  
  ... Passed --  15.3  5 1496  345033   16  
Test (2C): partitioned leader and one follower crash, leader restarts ...  
  ... Passed --   1.4  3   44   10942    4  
Test (2C): Figure 8 ...  
  ... Passed --  37.4  5 1584  336531   35  
Test (2C): unreliable agreement ...  
  ... Passed --   4.8  5  368  224592  246  
Test (2C): Figure 8 (unreliable) ...  
  ... Passed --  36.1  5 6340 19536185  168  
Test (2C): churn ...  
  ... Passed --  16.1  5 1236 2164271  429  
Test (2C): unreliable churn ...  
  ... Passed --  16.2  5 1364 1860223  150  
Test (2D): snapshots basic ...  
  ... Passed --   8.9  3  348  104646  251  
Test (2D): install snapshots (disconnect) ...  
  ... Passed --  92.1  3 3838  964048  402  
Test (2D): install snapshots (disconnect+unreliable) ...  
  ... Passed --  103.3  3 4368 1049753  369  
Test (2D): install snapshots (crash) ...  
  ... Passed --  44.6  3 1672  441411  388  
Test (2D): install snapshots (unreliable+crash) ...  
  ... Passed --  64.8  3 2392  593066  356  
PASS  
ok  	6.824/raft	492.332s  

Lab3: KVRaft  

Test: one client (3A) ...  
  ... Passed --  15.1  5  7039  589  
Test: ops complete fast enough (3A) ...  
  ... Passed --  25.5  3  8377    0  
Test: many clients (3A) ...  
  ... Passed --  15.4  5 23734 2351  
Test: unreliable net, many clients (3A) ...  
  ... Passed --  16.3  5  4910  943  
Test: concurrent append to same key, unreliable (3A) ...  
  ... Passed --   1.9  3   257   52  
Test: progress in majority (3A) ...  
  ... Passed --   0.4  5    99    2  
Test: no progress in minority (3A) ...  
  ... Passed --   1.1  5   346    3  
Test: completion after heal (3A) ...  
  ... Passed --   1.0  5   177    3  
Test: partitions, one client (3A) ...  
  ... Passed --  22.4  5 13757  411  
Test: partitions, many clients (3A) ...  
  ... Passed --  22.8  5 21404 1269  
Test: restarts, one client (3A) ...  
  ... Passed --  19.1  5 11024  588  
Test: restarts, many clients (3A) ...  
  ... Passed --  19.3  5 24780 2355  
Test: unreliable net, restarts, many clients (3A) ...  
  ... Passed --  20.2  5  5489 1024  
Test: restarts, partitions, many clients (3A) ...  
  ... Passed --  27.1  5 22494 1305  
Test: unreliable net, restarts, partitions, many clients (3A) ...  
  ... Passed --  27.4  5  5744  541  
Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...  
  ... Passed --  29.5  7 10852 1134  
Test: InstallSnapshot RPC (3B) ...  
  ... Passed --   3.4  3  5075   63  
Test: snapshot size is reasonable (3B) ...  
  ... Passed --  20.4  3  6574  800  
Test: ops complete fast enough (3B) ...  
  ... Passed --  25.5  3  7224    0  
Test: restarts, snapshots, one client (3B) ...  
  ... Passed --  19.1  5 18893  589  
Test: restarts, snapshots, many clients (3B) ...  
  ... Passed --  20.8  5 62987 2443  
Test: unreliable net, snapshots, many clients (3B) ...  
  ... Passed --  16.3  5  4672  939  
Test: unreliable net, restarts, snapshots, many clients (3B) ...  
  ... Passed --  21.3  5  5495  946  
Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...  
  ... Passed --  27.2  5  5254  501  
Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...  
  ... Passed --  29.8  7 10490  873  
PASS  
ok  	6.824/kvraft	448.701s  

Lab4: Shardctrler+Shardkv  

Test: Basic leave/join ...  
  ... Passed  
Test: Historical queries ...  
  ... Passed  
Test: Move ...  
  ... Passed  
Test: Concurrent leave/join ...  
  ... Passed  
Test: Minimal transfers after joins ...  
  ... Passed  
Test: Minimal transfers after leaves ...  
  ... Passed  
Test: Multi-group join/leave ...  
  ... Passed  
Test: Concurrent multi leave/join ...  
  ... Passed  
Test: Minimal transfers after multijoins ...  
  ... Passed  
Test: Minimal transfers after multileaves ...  
  ... Passed  
Test: Check Same config on servers ...  
  ... Passed  
PASS  
ok  	6.824/shardctrler	5.987s  

Test: static shards ...  
  ... Passed  
Test: join then leave ...  
  ... Passed  
Test: snapshots, join, and leave ...  
  ... Passed  
Test: servers miss configuration changes...  
  ... Passed  
Test: concurrent puts and configuration changes...  
  ... Passed  
Test: more concurrent puts and configuration changes...  
  ... Passed  
Test: concurrent configuration change and restart...  
  ... Passed  
Test: unreliable 1...  
  ... Passed  
Test: unreliable 2...  
  ... Passed  
Test: unreliable 3...  
  ... Passed  
Test: shard deletion (challenge 1) ...  
  ... Passed  
Test: unaffected shard access (challenge 2) ...  
  ... Passed  
Test: partial migration shard access (challenge 2) ...  
  ... Passed  
PASS  
ok  	6.824/shardkv	116.570s  
