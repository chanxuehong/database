package sql

import (
	"database/sql"
	"sync"
)

type DB struct {
	*sql.DB

	stmtSetRWMutex sync.RWMutex
	stmtSet        map[string]Stmt // map[query]*database/sql.Stmt
}

func NewDB(db *sql.DB) *DB {
	return &DB{
		DB:      db,
		stmtSet: make(map[string]Stmt),
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func (db *DB) Prepare(query string) (stmt Stmt, err error) {
	db.stmtSetRWMutex.RLock()
	stmt = db.stmtSet[query]
	db.stmtSetRWMutex.RUnlock()

	if stmt.Stmt != nil {
		return
	}

	db.stmtSetRWMutex.Lock()
	defer db.stmtSetRWMutex.Unlock()

	if stmt = db.stmtSet[query]; stmt.Stmt != nil {
		return
	}

	stmtx, err := db.DB.Prepare(query)
	if err != nil {
		return
	}
	stmt = Stmt{Stmt: stmtx}
	db.stmtSet[query] = stmt
	return
}

// =====================================================================================================================

type Stmt struct {
	*sql.Stmt
}

func (s Stmt) Close() error {
	return nil
}
