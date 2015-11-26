package sql

import (
	"database/sql"
	"sync"
)

type DB struct {
	*sql.DB

	stmtSetRWMutex sync.RWMutex
	stmtSet        map[string]*sql.Stmt // map[query]*sql.Stmt
}

func NewDB(db *sql.DB) *DB {
	return &DB{
		DB:      db,
		stmtSet: make(map[string]*sql.Stmt),
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

// NOTE: Never call sql.Stmt.Close().
func (db *DB) Prepare(query string) (stmt *sql.Stmt, err error) {
	db.stmtSetRWMutex.RLock()
	stmt = db.stmtSet[query]
	db.stmtSetRWMutex.RUnlock()

	if stmt != nil {
		return
	}

	db.stmtSetRWMutex.Lock()
	defer db.stmtSetRWMutex.Unlock()

	if stmt = db.stmtSet[query]; stmt != nil {
		return
	}

	stmt, err = db.DB.Prepare(query)
	if err != nil {
		return
	}
	db.stmtSet[query] = stmt
	return
}
