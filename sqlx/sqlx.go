package sqlx

import (
	"database/sql"
	"sync"

	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB

	stmtSetRWMutex sync.RWMutex
	stmtSet        map[string]Stmt // map[query]*sql.Stmt

	stmtxSetRWMutex sync.RWMutex
	stmtxSet        map[string]Stmtx // map[query]*sqlx.Stmt

	namedStmtSetRWMutex sync.RWMutex
	namedStmtSet        map[string]NamedStmt // map[query]*sqlx.NamedStmt
}

func NewDB(db *sqlx.DB) *DB {
	return &DB{
		DB:           db,
		stmtSet:      make(map[string]Stmt),
		stmtxSet:     make(map[string]Stmtx),
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

func (db *DB) Preparex(query string) (stmt Stmtx, err error) {
	db.stmtxSetRWMutex.RLock()
	stmt = db.stmtxSet[query]
	db.stmtxSetRWMutex.RUnlock()

	if stmt.Stmt != nil {
		return
	}

	db.stmtxSetRWMutex.Lock()
	defer db.stmtxSetRWMutex.Unlock()

	if stmt = db.stmtxSet[query]; stmt.Stmt != nil {
		return
	}

	stmtx, err := db.DB.Preparex(query)
	if err != nil {
		return
	}
	stmt = Stmtx{Stmt: stmtx}
	db.stmtxSet[query] = stmt
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
	*sql.Stmt
}

func (s Stmt) Close() error {
	return nil
}

type Stmtx struct {
	*sqlx.Stmt
}

func (s Stmtx) Close() error {
	return nil
}

type NamedStmt struct {
	*sqlx.NamedStmt
}

func (s NamedStmt) Close() error {
	return nil
}
