```
$ go run ./cmd/analyzeplan -ddl-file schema.sql < test.json 
Possibly back join table Singers at [0 1 13 14 19 20 21] and index SingersByFirstLastName at [0 1 2 3 4 5] at: 1
Possibly because used columns is not contained in the index: [BirthDate SingerInfo]
  Candidate DDL to avoid back join: CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName) STORING (BirthDate, SingerInfo)
Residual condition at FilterScan(20) is possibly because used columns is not contained, index: SingersByFirstLastName, missing columns: []
  Candidate DDL to optimize filter: CREATE INDEX SingersByFirstLastName ON Singers(FirstName, LastName)
```
