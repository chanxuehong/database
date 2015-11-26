package sqlx

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB

	stmtSetRWMutex sync.RWMutex
	stmtSet        map[string]*sqlx.Stmt // map[query]*sqlx.Stmt

	namedStmtSetRWMutex sync.RWMutex
	namedStmtSet        map[string]*sqlx.NamedStmt // map[query]*sqlx.NamedStmt
}

func NewDB(db *sqlx.DB) *DB {
	return &DB{
		DB:           db,
		stmtSet:      make(map[string]*sqlx.Stmt),
		namedStmtSet: make(map[string]*sqlx.NamedStmt),
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

// NOTE: Never call sqlx.Stmt.Close() and sqlx.Stmt.Stmt.Close().
func (db *DB) Preparex(query string) (stmt *sqlx.Stmt, err error) {
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

	stmt, err = db.DB.Preparex(query)
	if err != nil {
		return
	}
	db.stmtSet[query] = stmt
	return
}

// NOTE: Never call sqlx.NamedStmt.Close() and sqlx.NamedStmt.Stmt.Close().
func (db *DB) PrepareNamed(query string) (stmt *sqlx.NamedStmt, err error) {
	db.namedStmtSetRWMutex.RLock()
	stmt = db.namedStmtSet[query]
	db.namedStmtSetRWMutex.RUnlock()

	if stmt != nil {
		return
	}

	db.namedStmtSetRWMutex.Lock()
	defer db.namedStmtSetRWMutex.Unlock()

	if stmt = db.namedStmtSet[query]; stmt != nil {
		return
	}

	stmt, err = db.DB.PrepareNamed(query)
	if err != nil {
		return
	}
	db.namedStmtSet[query] = stmt
	return
}
