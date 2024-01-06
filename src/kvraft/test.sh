for i in {1..20}; 
    do go test -run 3; 
    # go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B; 
done
