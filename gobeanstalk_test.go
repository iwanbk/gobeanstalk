package gobeanstalk

import "testing"

const (
	testtube = "testtube"
	testjob  = "testjob"
)

func dial(t *testing.T) *Conn {
	conn, err := Dial("localhost:11300")
	if err != nil {
		t.Fatal("Dial failed.err = :", err.Error())
	}
	return conn
}

func TestDial(t *testing.T) {
	if _, err := Dial("localhost:11300") ; err != nil {
		t.Fatal("Dial failed.err = :", err.Error())
	}
}

func TestUse(t *testing.T) {
	conn := dial(t)
	err := conn.Use(testtube)
	if err != nil {
		t.Fatal("use failed.Err = ", err.Error())
	}
}

func TestPut(t *testing.T) {
	conn := dial(t)
	err := conn.Use(testtube)
	if err != nil {
		t.Fatal("use failed.Err = ", err.Error())
	}
	_, err = conn.Put([]byte("testjob"), 0, 0, 0)
	if err != nil {
		t.Fatal("Put failed. Err = ", err.Error())
	}
}

func watch(t *testing.T) *Conn {
	conn := dial(t)
	_, err := conn.Watch(testtube)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
func TestWatch(t *testing.T) {
	watch(t)
}

func reserve(t *testing.T) *Job {
	conn := watch(t)
	j, err := conn.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if string(j.Body) != testjob {
		t.Fatal("job body check failed")
	}
	return j
}
func TestReserve(t *testing.T) {
	reserve(t)
}

func TestDelete(t *testing.T) {
	j := reserve(t)
	err := j.Delete()
	if err != nil {
		t.Error("delete failed. Err = ", err.Error())
	}
}
