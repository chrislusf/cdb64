package main

import (
	"log"

	"github.com/chrislusf/cdb64"
)

var expectedRecords = [][][]byte{
	{[]byte("foo"), []byte("bar")},
	{[]byte("baz"), []byte("quuuux")},
	{[]byte("playwright"), []byte("wow")},
	{[]byte("crystal"), []byte("CASTLES")},
	{[]byte("CRYSTAL"), []byte("castles")},
	{[]byte("snush"), []byte("collision!")}, // 'playwright' collides with 'snush' in cdbhash
	{[]byte("a"), []byte("a")},
	{[]byte("empty_value"), []byte("")},
	{[]byte(""), []byte("empty_key")},
	{[]byte("not in the table"), nil},
}

func main() {
	writer, err := cdb64.Create("test.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Write some key/value pairs to the database.
	for _, pair := range expectedRecords {
		writer.Put(pair[0], pair[1])
	}

	// Freeze the database, and open it for reads.
	db, err := writer.Freeze()
	if err != nil {
		log.Fatal(err)
	}

	// Fetch a value.
	v, err := db.Get([]byte("crystal"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println(string(v))
	// => Practice

	// Iterate over the database
	iter := db.Iter()
	for iter.Next() {
		log.Printf("The key %s has a value of length %d\n", string(iter.Key()), len(iter.Value()))
	}

	if err := iter.Err(); err != nil {
		log.Fatal(err)
	}

}
