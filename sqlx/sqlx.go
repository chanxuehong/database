package sqlx

import (
	"sync"

	"github.com/chanxuehong/database/sql"
	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB

	sqlStmtSetRWMutex sync.RWMutex
	sqlStmtSet        map[string]sql.Stmt // map[query]*database/sql.Stmt

	stmtSetRWMutex sync.RWMutex
	stmtSet        map[string]Stmt // map[query]*github.com/jmoiron/sqlx.Stmt

	namedStmtSetRWMutex sync.RWMutex
	namedStmtSet        map[string]NamedStmt // map[query]*github.com/jmoiron/sqlx.NamedStmt
}

func NewDB(db *sqlx.DB) *DB {
	return &DB{
		DB:           db,
		sqlStmtSet:   make(map[string]sql.Stmt),
		stmtSet:      make(map[string]Stmt),
		namedStmtSet: make(map[string]NamedStmt),
	}
}

func Open(driverName, dataSourceName string) (*DB, error) {
	db, err := sqlx.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return NewDB(db), nil
}

func (db *DB) Prepare(query string) (stmt sql.Stmt, err error) {
	db.sqlStmtSetRWMutex.RLock()
	stmt = db.sqlStmtSet[query]
	db.sqlStmtSetRWMutex.RUnlock()

	if stmt.Stmt != nil {
		return
	}

	db.sqlStmtSetRWMutex.Lock()
	defer db.sqlStmtSetRWMutex.Unlock()

	if stmt = db.sqlStmtSet[query]; stmt.Stmt != nil {
		return
	}

	stmtx, err := db.DB.Prepare(query)
	if err != nil {
		return
	}
	stmt = sql.Stmt{Stmt: stmtx}
	db.sqlStmtSet[query] = stmt
	return
}

func (db *DB) Preparex(query string) (stmt Stmt, err error) {
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

	stmtx, err := db.DB.Preparex(query)
	if err != nil {
		return
	}
	stmt = Stmt{Stmt: stmtx}
	db.stmtSet[query] = stmt
	return
}

func (db *DB) PrepareNamed(query string) (stmt NamedStmt, err error) {
	db.namedStmtSetRWMutex.RLock()
	stmt = db.namedStmtSet[query]
	db.namedStmtSetRWMutex.RUnlock()

	if stmt.NamedStmt != nil {
		return
	}

	db.namedStmtSetRWMutex.Lock()
	defer db.namedStmtSetRWMutex.Unlock()

	if stmt = db.namedStmtSet[query]; stmt.NamedStmt != nil {
		return
	}

	stmtx, err := db.DB.PrepareNamed(query)
	if err != nil {
		return
	}
	stmt = NamedStmt{NamedStmt: stmtx}
	db.namedStmtSet[query] = stmt
	return
}

// =====================================================================================================================

type Stmt struct {
	*sqlx.Stmt
}

func (s Stmt) Close() error {
	return nil
}

type NamedStmt struct {
	*sqlx.NamedStmt
}

func (s NamedStmt) Close() error {
	return nil
}
