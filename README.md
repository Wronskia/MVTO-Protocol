# Description

You can find an implementation of a main-memory key-value store with Multi-Version Timestamp Ordering (MVTO) protocol.
MVTO is similar to the MVCC protocol. The main difference is that write
operations are not buffered. Thus, reading an uncommitted value is possible and that could
cause cascading aborts. 

MVTO Specification:
When a transaction Ti starts, MVTO assigns a timestamp TS(Ti) to it.
For every database object O, MVTO keeps a sequence of versions <O1, O2, … , On>. Each
version Ok contains three data fields:
- Content: the value of Ok.
- WTS(Ok): the timestamp of the transaction that inserted (or wrote) Ok.
- RTS(Ok): the largest timestamp among all the timestamps of the transactions that
have read Ok.
Actions:
- Reads: Reads in MVTO always succeed and a transaction never waits as a version of
the requested object is returned immediately.
Specifically, suppose that transaction Ti issues a read(O) operation.
1) The version Ok that is returned is the one with the largest timestamp less than or
equal to the timestamp of the transaction Ti (TS(Ti)).
2) If TS(Ti) > RTS(Ok), the RTS(Ok) is updated with the timestamp of the transaction
Ti.
- Writes: Suppose that transaction Ti issues a write(O) operation.
1) The version Ok with the largest write timestamp less than or equal to TS(Ti) is
found.
2) If TS(Ti) < RTS(Ok), the write is rejected and Ti is rolled back.
3) If TS(Ti) ≥ RTS(Ok) and TS(Ti) = WTS(Ok), the contents of Ok are overwritten.
4) If TS(Ti) ≥ RTS(Ok) and TS(Ti) > WTS(Ok), a new version Om of O is created, having
RTS(Om) = WTS(Om) = TS(Ti).
- Commits: Processing the commit of transaction Ti is delayed for recoverability, until
all transactions (Tj , j != i) that wrote versions read by Ti have successfully committed.
If any of the transactions Tj aborts, Ti should abort as well. 
# Author

Yassine Benyahia
