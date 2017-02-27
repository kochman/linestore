package main

import (
	"log"
	"os"
	"strconv"
    "time"
    "bytes"
    "io/ioutil"
    "io"
    "net/http"
    "encoding/json"
    "strings"
    "github.com/ugorji/go/codec"
)

type LSRecord struct {
    Id string
    Timestamp time.Time
    Event string
    Value string
}

type LSFile struct {
	f *os.File
}

func (ls *LSFile) Write(data []byte) {
    // seek to end of file
	_, err := ls.f.Seek(0, 2)
	if err != nil {
		log.Fatal(err)
	}

	n, err := ls.f.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	log.Print("[INFO] Wrote " + strconv.Itoa(n) + " bytes")
}

func (ls *LSFile) WriteRecord(record LSRecord) {
    buf := &bytes.Buffer{}

    mh := &codec.MsgpackHandle{}
    enc := codec.NewEncoder(buf, mh)
    err := enc.Encode(record)
    if err != nil {
        log.Fatal(err)
    }

    ls.Write(buf.Bytes())
}

func (ls *LSFile) ReadRecords() (records []LSRecord) {
    data := ls.read()
    buf := bytes.NewBuffer(data)

    for {
        mh := &codec.MsgpackHandle{}
        enc := codec.NewDecoder(buf, mh)
        record := &LSRecord{}
        err := enc.Decode(record)
        if err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        }
        records = append(records, *record)
    }
    return
}

func (ls *LSFile) read() (data []byte) {
    // seek to beginning of file
	_, err := ls.f.Seek(0, 0)
	if err != nil {
		log.Fatal(err)
	}

	data, err = ioutil.ReadAll(ls.f)
	if err != nil {
		log.Fatal(err)
	}

    // chop off header
    recordStart := bytes.Index(data, []byte("\n\n"))
    data = data[recordStart+2:]

    return
}

func (ls *LSFile) ReadRecordsForId(id string) (records []LSRecord) {
    for _, record := range ls.ReadRecords() {
        if record.Id == id {
            records = append(records, record)
        }
    }
    return
}

func (ls *LSFile) WriteHeader() {
    var header string

    header += "linestore-lsfile/v1\n"
    header += "\n"

    ls.Write([]byte(header))
}

func createFile() (ls *LSFile) {
	f, err := os.Create("linestore.ls")
	if err != nil {
		log.Fatal(err)
	}

	ls = &LSFile{f: f}

    ls.WriteHeader()

	return
}

func openFile() (ls *LSFile) {
	f, err := os.OpenFile("linestore.ls", os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	ls = &LSFile{f: f}
	return
}

// HTTP interface

func httpReadRecords(w http.ResponseWriter, r *http.Request) {
    buf := &bytes.Buffer{}
    enc := json.NewEncoder(buf)

    ls := openFile()
    err := enc.Encode(ls.ReadRecords())
    if err != nil {
        log.Fatal(err)
    }

    io.Copy(w, buf)
}

func httpReadRecordsForId(w http.ResponseWriter, r *http.Request) {
    url := r.URL
    path := strings.Split(url.Path, "/")
    if len(path) == 5 {
        httpCreateRecordForId(w, r)
        return
    }
    id := path[2]


    buf := &bytes.Buffer{}
    enc := json.NewEncoder(buf)

    ls := openFile()
    err := enc.Encode(ls.ReadRecordsForId(id))
    if err != nil {
        log.Fatal(err)
    }

    io.Copy(w, buf)
}

func httpCreateRecordForId(w http.ResponseWriter, r *http.Request) {
    url := r.URL
    path := strings.Split(url.Path, "/")
    id := path[2]
    event := path[3]
    value := path[4]

    record := &LSRecord{
        Id: id,
        Timestamp: time.Now(),
        Event: event,
        Value: value,
    }

    ls := openFile()
    ls.WriteRecord(*record)

    buf := &bytes.Buffer{}
    enc := json.NewEncoder(buf)
    err := enc.Encode(record)
    if err != nil {
        log.Fatal(err)
    }

    io.Copy(w, buf)
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	log.Print("linestore started")

	ls := createFile()

    for _, val := range []string{"hello", "world", "these", "are", "records"} {
        record := LSRecord{
            Id: "example",
            Timestamp: time.Now(),
            Event: "event",
            Value: val,
        }
        ls.WriteRecord(record)
    }

    numRecords := len(ls.ReadRecords())
    log.Print("[INFO] " + strconv.Itoa(numRecords) + " records stored")

    http.HandleFunc("/records", httpReadRecords)
    http.HandleFunc("/records/", httpReadRecordsForId)
    log.Fatal(http.ListenAndServe("127.0.0.1:8009", nil))
}
