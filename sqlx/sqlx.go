package sqlx

import (
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/chanxuehong/database/sql"
	"github.com/jmoiron/sqlx"
)

type DB struct {
	*sqlx.DB

	sqlStmtMapPtrMutex sync.Mutex     // used only by writers
	sqlStmtMapPtr      unsafe.Pointer // *sqlStmtMap

	stmtMapPtrMutex sync.Mutex     // used only by writers
	stmtMapPtr      unsafe.Pointer // *stmtMap

	namedStmtMapPtrMutex sync.Mutex     // used only by writers
	namedStmtMapPtr      unsafe.Pointer // *namedStmtMap
}

type (
	sqlStmtMap   map[string]sql.Stmt  // map[query]sql.Stmt
	stmtMap      map[string]Stmt      // map[query]Stmt
	namedStmtMap map[string]NamedStmt // map[query]NamedStmt
)

func NewDB(db *sqlx.DB) *DB {
	return &DB{
		DB: db,
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
	var m sqlStmtMap
	if p := (*sqlStmtMap)(atomic.LoadPointer(&db.sqlStmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	db.sqlStmtMapPtrMutex.Lock()
	defer db.sqlStmtMapPtrMutex.Unlock()

	if p := (*sqlStmtMap)(atomic.LoadPointer(&db.sqlStmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	stmtx, err := db.DB.Prepare(query)
	if err != nil {
		return
	}
	stmt = sql.Stmt{Stmt: stmtx}

	m2 := make(sqlStmtMap, len(m)+1)
	for k, v := range m {
		m2[k] = v
	}
	m2[query] = stmt

	atomic.StorePointer(&db.sqlStmtMapPtr, unsafe.Pointer(&m2))
	return
}

func (db *DB) Preparex(query string) (stmt Stmt, err error) {
	var m stmtMap
	if p := (*stmtMap)(atomic.LoadPointer(&db.stmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	db.stmtMapPtrMutex.Lock()
	defer db.stmtMapPtrMutex.Unlock()

	if p := (*stmtMap)(atomic.LoadPointer(&db.stmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	stmtx, err := db.DB.Preparex(query)
	if err != nil {
		return
	}
	stmt = Stmt{Stmt: stmtx}

	m2 := make(stmtMap, len(m)+1)
	for k, v := range m {
		m2[k] = v
	}
	m2[query] = stmt

	atomic.StorePointer(&db.stmtMapPtr, unsafe.Pointer(&m2))
	return
}

func (db *DB) PrepareNamed(query string) (stmt NamedStmt, err error) {
	var m namedStmtMap
	if p := (*namedStmtMap)(atomic.LoadPointer(&db.namedStmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	db.namedStmtMapPtrMutex.Lock()
	defer db.namedStmtMapPtrMutex.Unlock()

	if p := (*namedStmtMap)(atomic.LoadPointer(&db.namedStmtMapPtr)); p != nil {
		m = *p
		if stmt = m[query]; stmt.Stmt != nil {
			return
		}
	}

	stmtx, err := db.DB.PrepareNamed(query)
	if err != nil {
		return
	}
	stmt = NamedStmt{NamedStmt: stmtx}

	m2 := make(namedStmtMap, len(m)+1)
	for k, v := range m {
		m2[k] = v
	}
	m2[query] = stmt

	atomic.StorePointer(&db.namedStmtMapPtr, unsafe.Pointer(&m2))
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
