/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

 package monetdb

 import (
	"context"
	"database/sql"
	"strings"
	 "testing"
 )
 
func TestTxIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test4 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Start transaction", func(t *testing.T) {
		tx, err := db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.Exec("insert into test4 values ( 1, 'name1' )")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != -1 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
		if err := tx.Commit(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("select * from test4")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test4")
		if err != nil {
			t.Fatal(err)
		}
	})

	defer db.Close()
}

func TestTxReadOnlyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var (
		ctx context.Context
		db  *sql.DB
		tx *sql.Tx
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.PingContext(ctx); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test4 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Start transaction", func(t *testing.T) {
		opts := sql.TxOptions{Isolation: sql.LevelDefault, ReadOnly: true}
		tx, err := db.BeginTx(ctx, &opts)
		if err == nil {
			t.Fatal("this transaction should have failed")
		}
		if strings.Trim(err.Error(), "\n") != "mapi: operational error: 42000!Readonly transactions not supported" {
			t.Error("unexpected error message: ", err)
		}
		if tx != nil {
			t.Fatal("transaction was expected to be nil")
		}
	})

	t.Run("rollback transaction", func(t *testing.T) {
		if tx != nil {
			if err := tx.Rollback(); err != nil {
				t.Error(err)
			}
		}
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test4")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestTxSerializableIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	var (
		ctx context.Context
		db  *sql.DB
	)

	db, err := sql.Open("monetdb", "monetdb:monetdb@localhost:50000/monetdb")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx = context.Background()

	if pingErr := db.Ping(); pingErr != nil {
		t.Fatal(pingErr)
	}

	t.Run("Exec create table", func(t *testing.T) {
		_, err := db.Exec("create table test4 ( id int, name varchar(16))")
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Start transaction", func(t *testing.T) {
		opts := sql.TxOptions{Isolation: sql.LevelSerializable}
		tx, err := db.BeginTx(ctx, &opts)
		if err != nil {
			t.Fatal(err)
		}
		result, err := tx.Exec("insert into test4 values ( 1, 'name1' )")
		if err != nil {
			t.Fatal(err)
		}
		if result == nil {
			t.Fatal("query did not return a result object")
		}
		rId, err := result.LastInsertId()
		if err != nil {
			t.Error("Could not get id from result")
		}
		if rId != -1 {
			t.Errorf("Unexpected id %d", rId)
		}
		nRows, err := result.RowsAffected()
		if err != nil {
			t.Error("Could not get number of rows from result")
		}
		if nRows != 1 {
			t.Errorf("Unexpected number of rows %d", nRows)
		}
		if err := tx.Commit(); err != nil {
			t.Error(err)
		}
	})

	t.Run("Run simple query", func(t *testing.T) {
		rows, err := db.Query("select * from test4")
		if err != nil {
			t.Fatal(err)
		}
		if rows == nil {
			t.Fatal("empty result")
		}
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				t.Error(err)
			}
		}
		if err := rows.Err(); err != nil {
			t.Error(err)
		}
		defer rows.Close()
	})

	t.Run("Exec drop table", func(t *testing.T) {
		_, err := db.Exec("drop table test4")
		if err != nil {
			t.Fatal(err)
		}
	})
}
