package gobeanstalk

import (
	"testing"
	"time"
)

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
	if _, err := Dial("localhost:11300"); err != nil {
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

func put(t *testing.T, tubename string, jobBody string) {
	conn := dial(t)
	err := conn.Use(tubename)
	if err != nil {
		t.Fatal("use failed.Err = ", err.Error())
	}
	_, err = conn.Put([]byte(jobBody), 0, 2*time.Second, 30*time.Second)
	if err != nil {
		t.Fatal("Put failed. Err = ", err.Error())
	}
}

func TestPut(t *testing.T) {
	put(t, testtube, testjob)
}

func watch(t *testing.T, tubename string) *Conn {
	conn := dial(t)
	_, err := conn.Watch(tubename)
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
func TestWatch(t *testing.T) {
	watch(t, testtube)
}

func reserve(t *testing.T, tubename string, timeout ...time.Duration) (*Conn, *Job) {
	conn := watch(t, tubename)
    var j *Job
    var err error
    if len(timeout) > 0 {
        j, err = conn.Reserve(timeout[0])
    } else {
        j, err = conn.Reserve()
    }
    
    if err == ErrTimedOut {
        return nil, nil
    }
	if err != nil {
		t.Fatal(err)
	}
	if string(j.Body) != testjob {
		t.Fatal("job body check failed")
	}
	return conn, j
}
func TestReserve(t *testing.T) {
	conn, j := reserve(t, testtube)
    reserve(t, testtube, 2*time.Second)
    conn.Release(j.ID, 0, 0*time.Second)
}

func statsJob(t *testing.T, tubename string) {
	conn, j := reserve(t, testtube)
	yaml, err := conn.StatsJob(j.ID)
	if err != nil {
		t.Fatal("StatsJob failed.Err = ", err.Error())
	}
	t.Log(string(yaml))
    conn.Release(j.ID, 0, 0*time.Second)
}
func TestStatsJob(t *testing.T) {
	statsJob(t, testtube)
}

func TestDelete(t *testing.T) {
	conn, j := reserve(t, testtube)
	err := conn.Delete(j.ID)
	if err != nil {
		t.Error("delete failed. Err = ", err.Error())
	}
}
