```
> EXPLAIN SELECT * FROM Singers@{FORCE_INDEX=SingersByFirstLastName} WHERE BirthDate = CURRENT_DATE();
+-----+----------------------------------------------------------------------------+
| ID  | Query_Execution_Plan (EXPERIMENTAL)                                        |
+-----+----------------------------------------------------------------------------+
|   0 | Distributed Union                                                          |
|  *1 | +- Distributed Cross Apply                                                 |
|   2 |    +- [Input] Create Batch                                                 |
|   3 |    |  +- Local Distributed Union                                           |
|   4 |    |     +- Compute Struct                                                 |
|   5 |    |        +- Index Scan (Full scan: true, Index: SingersByFirstLastName) |
|  13 |    +- [Map] Serialize Result                                               |
|  14 |       +- Cross Apply                                                       |
|  15 |          +- [Input] Batch Scan (Batch: $v2)                                |
|  19 |          +- [Map] Local Distributed Union                                  |
| *20 |             +- FilterScan                                                  |
|  21 |                +- Table Scan (Table: Singers)                              |
+-----+----------------------------------------------------------------------------+
Predicates(identified by ID):
  1: Split Range: ($SingerId' = $SingerId)
 20: Seek Condition: ($SingerId' = $batched_SingerId)
     Residual Condition: ($BirthDate = CURRENT_DATE())

$ go run ./cmd/analyzeplan -ddl-file schema.sql < test.json 
Possibly back join table Singers at [0 1 13 14 19 20 21] and index SingersByFirstLastName at [0 1 2 3 4 5] at: 1
Possibly because used columns is not contained in the index: [BirthDate SingerInfo]
  Candidate DDL to avoid back join: CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName) STORING (BirthDate, SingerInfo)
Residual condition at FilterScan(20) is possibly because used columns is not contained, index: SingersByFirstLastName, missing columns: [BirthDate]
  Candidate DDL to optimize filter: CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName) STORING (BirthDate)
```
