package cdb64

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestGet(t *testing.T) {
	db, err := Open("./test/test.cdb")
	require.NoError(t, err)
	require.NotNil(t, db)

	records := append(append(expectedRecords, expectedRecords...), expectedRecords...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])

		value, err := db.Get(record[0])
		require.NoError(t, err, msg)
		println("key:", string(record[0]), "value:", string(value), "nil:", value == nil)
		assert.Equal(t, string(record[1]), string(value), msg)
	}
}

func TestGetParallel(t *testing.T) {
	db, err := Open("./test/test.cdb")
	require.NoError(t, err)
	require.NotNil(t, db)

	records := append(append(expectedRecords, expectedRecords...), expectedRecords...)
	shuffle(records)

	for _, record := range records {
		msg := "while fetching " + string(record[0])

		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(t *testing.T) {
				defer wg.Done()
				value, err := db.Get(record[0])
				require.NoError(t, err, msg)
				println("key:", string(record[0]), "value:", string(value), "nil:", value == nil)
				assert.Equal(t, string(record[1]), string(value), msg)
			}(t)
		}
		wg.Wait()
	}
}

func TestClosesFile(t *testing.T) {
	f, err := os.Open("./test/test.cdb")
	require.NoError(t, err)

	db, err := New(f, nil)
	require.NoError(t, err)
	require.NotNil(t, db)

	err = db.Close()
	require.NoError(t, err)

	err = f.Close()
	assert.Equal(t, syscall.EINVAL, err)
}

func BenchmarkGet(b *testing.B) {
	db, _ := Open("./test/test.cdb")
	b.ResetTimer()

	rand.Seed(time.Now().UnixNano())
	for i := 0; i < b.N; i++ {
		record := expectedRecords[rand.Intn(len(expectedRecords))]
		db.Get(record[0])
	}
}

func Example() {
	writer, err := Create("/tmp/example.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Write some key/value pairs to the database.
	writer.Put([]byte("Alice"), []byte("Practice"))
	writer.Put([]byte("Bob"), []byte("Hope"))
	writer.Put([]byte("Charlie"), []byte("Horse"))

	// Freeze the database, and open it for reads.
	db, err := writer.Freeze()
	if err != nil {
		log.Fatal(err)
	}

	// Fetch a value.
	v, err := db.Get([]byte("Alice"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(v))
	// Output: Practice
}

func ExampleCDB() {
	db, err := Open("./test/test.cdb")
	if err != nil {
		log.Fatal(err)
	}

	// Fetch a value.
	v, err := db.Get([]byte("foo"))
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(v))
	// Output: bar
}

func shuffle(a [][][]byte) {
	rand.Seed(time.Now().UnixNano())
	for i := len(a) - 1; i > 0; i-- {
		j := rand.Intn(i)
		a[i], a[j] = a[j], a[i]
	}
}
